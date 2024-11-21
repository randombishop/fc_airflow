import datetime
import airflow
import pandas
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from dune_client.types import QueryParameter
from airflow.hooks.postgres_hook import PostgresHook
from utils import exec_dune_query, dataframe_to_gcs, dataframe_to_dune
import logging


def pull_casts(**context):
  ts_from = context['logical_date'].strftime('%Y-%m-%d %H')+':00:00'
  ts_to = (context['logical_date'] + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H')+':00:00'
  logging.info(f"Pulling casts from {ts_from} to {ts_to}")
  params = [
    QueryParameter.text_type(name="ts_from", value=ts_from),
    QueryParameter.text_type(name="ts_to", value=ts_to)
  ]
  query_id = 4188892
  df = exec_dune_query(query_id, params)
  if len(df) < 1000:
    raise Exception(f"Dataframe is too small: {len(df)}")
  tag = context['logical_date'].strftime("%Y-%m-%d-%H")
  destination = f'pipelines/dune/casts/{tag}_casts.csv'
  dataframe_to_gcs(df, destination)


def push_casts(**context):
  key = context['logical_date'].strftime('%Y-%m-%d-%H')
  local_file = key + '_bird.csv'
  remote_file = 'pipelines/dune/bird/' + local_file
  bucket_name = 'dsart_nearline1'
  gcs_hook = GCSHook()
  gcs_hook.download(bucket_name=bucket_name, object_name=remote_file, filename=local_file)
  df = pandas.read_csv(local_file)
  df['fid'] = df['fid'].astype(int)
  logging.info(f"Dataframe fetched from GCS: {len(df)}")
  logging.info(f"Dataframe columns: {list(df.columns)}")
  dataframe_to_dune(df, 'dsart', 'casts_features')
  if os.path.exists(local_file):
    os.remove(local_file)
  logging.info(f"Removed local file")
  

def update_channel_counts(**context):
  ts_from = context['logical_date'].strftime('%Y-%m-%d %H')+':00:00'
  ts_to = (context['logical_date'] + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H')+':00:00'
  logging.info(f"Pulling channel counts from {ts_from} to {ts_to}")
  params = [
    QueryParameter.text_type(name="ts_from", value=ts_from),
    QueryParameter.text_type(name="ts_to", value=ts_to)
  ]
  query_id = 4259101
  df = exec_dune_query(query_id, params)
  logging.info(f"Dataframe fetched from Dune: {len(df)}")
  pg_hook = PostgresHook(postgres_conn_id='pg_dsart')
  engine = pg_hook.get_sqlalchemy_engine()
  with engine.connect() as connection:
    df.to_sql('tmp_channel_activity', connection, if_exists='replace', index=False)
    logging.info(f"Uploaded to temp table tmp_channel_activity")
    sql1 = """UPDATE ds.channels_digest AS t
             SET num_casts = t.num_casts + s.num_casts
             FROM tmp_channel_activity s
             WHERE t.url = s.channel ;"""
    connection.execute(sql1)
    logging.info(f"Executed SQL: {sql1}")
    sql2 = """INSERT INTO ds.channels_digest (url, num_casts) (
            SELECT channel as url, num_casts FROM tmp_channel_activity
            WHERE channel not in (select url from ds.channels_digest)
          ) ;""" 
    connection.execute(sql2)
    logging.info(f"Executed SQL: {sql2}")
    sql_drop = "DROP TABLE tmp_channel_activity ;"
    connection.execute(sql_drop)
    logging.info(f"Dropped temp table tmp_channel_activity")
  logging.info(f"Done")
  



default_args = {
  'start_date': airflow.utils.dates.days_ago(2),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
  'casts_features',
  default_args=default_args,
  description='Process casts hourly',
  schedule_interval='45 */2 * * *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

  casts = PythonOperator(
    task_id='casts',
    python_callable=pull_casts,
    provide_context=True,
  )
  
  embeddings = SSHOperator(
    task_id='embeddings',
    ssh_conn_id='ssh_worker',
    command='/home/na/embeddings.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=300,
    get_pty=True)
  
  gambit = SSHOperator(
    task_id='gambit',
    ssh_conn_id='ssh_worker',
    command='/home/na/gambit2.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=120,
    get_pty=True)
  
  join = SSHOperator(
    task_id='join',
    ssh_conn_id='ssh_worker',
    command='/home/na/join1.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=120,
    get_pty=True)
  
  bird = SSHOperator(
    task_id='bird',
    ssh_conn_id='ssh_worker',
    command='/home/na/bird2.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=900,
    get_pty=True)
  
  push = PythonOperator(
    task_id='push',
    python_callable=push_casts,
    provide_context=True,
  )
  
  update_channels = PythonOperator(
    task_id='update_channels',
    python_callable=update_channel_counts,
    provide_context=True,
  )
  
  casts >> embeddings >> gambit >> join >> bird >> push >> update_channels