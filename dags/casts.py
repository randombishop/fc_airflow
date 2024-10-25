import datetime
import airflow
import pandas
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from dune_client.types import QueryParameter
from utils import exec_dune_query, dataframe_to_gcs, dataframe_to_dune
import logging


def pull_casts(**context):
  ts_from = context['logical_date'].strftime('%Y-%m-%d %H:%M:%S')
  ts_to = (context['logical_date'] + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
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
  logging.info(f"Dataframe fetched from GCS: {len(df)}")
  dataframe_to_dune(df, 'dsart', 'casts_features')
  if os.path.exists(local_file):
    os.remove(local_file)
  logging.info(f"Removed local file")
  


default_args = {
  'start_date': airflow.utils.dates.days_ago(2),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
  'casts',
  default_args=default_args,
  description='Process casts hourly',
  schedule_interval='0 * * * *',
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
    cmd_timeout=60,
    get_pty=True)
  
  bird = SSHOperator(
    task_id='bird',
    ssh_conn_id='ssh_worker',
    command='/home/na/bird2.sh "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=60,
    get_pty=True)
  
  push = PythonOperator(
    task_id='push',
    python_callable=push_casts,
    provide_context=True,
  )
  
  casts >> embeddings >> gambit >> join >> bird >> push