import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from dune_client.types import QueryParameter
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


default_args = {
  'start_date': airflow.utils.dates.days_ago(2),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
  'casts_preprocess',
  default_args=default_args,
  description='Calculates embeddings and detects language for each cast',
  schedule_interval='40 */2 * * *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

  casts = PythonOperator(
    task_id='casts',
    python_callable=pull_casts,
    provide_context=True,
  )
  
  embed = SSHOperator(
    task_id='embed',
    ssh_conn_id='ssh_worker',
    command='/home/na/worker.sh embeddings run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=600,
    get_pty=True)
  
  lang = SSHOperator(
    task_id='lang',
    ssh_conn_id='ssh_worker',
    command='/home/na/worker.sh detect_lang run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=600,
    get_pty=True)
  
  
  casts >> embed >> lang