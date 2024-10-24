import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from dune_client.types import QueryParameter
from utils import exec_dune_query, dataframe_to_gcs
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
    cmd_timeout=300,
    get_pty=True)
  
  casts >> embeddings >> gambit