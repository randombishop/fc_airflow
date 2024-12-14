import pandas
import os
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utils import dataframe_to_dune
import logging


def push_casts(**context):
  key = context['logical_date'].strftime('%Y-%m-%d-%H')
  local_file = key + '_bird.csv'
  remote_file = 'pipelines/dune/bird/' + local_file
  bucket_name = 'dsart_nearline1'
  gcs_hook = GCSHook()
  gcs_hook.download(bucket_name=bucket_name, object_name=remote_file, filename=local_file)
  df = pandas.read_csv(local_file)
  df['fid'] = df['fid'].astype(int)
  df['category'] = df['category'].astype('Int64')
  df['topic'] = df['topic'].astype('Int64')
  logging.info(f"Dataframe fetched from GCS: {len(df)}")
  logging.info(f"Dataframe columns: {list(df.columns)}")
  dataframe_to_dune(df, 'dsart', 'casts_features')
  if os.path.exists(local_file):
    os.remove(local_file)
  logging.info(f"Removed local file")
  

def create_task_group(dag):
  with TaskGroup(group_id='casts_features2', dag=dag) as dag:
    gambit = SSHOperator(
      task_id='gambit',
      ssh_conn_id='ssh_worker',
      command='/home/na/worker.sh gambit2 run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
      cmd_timeout=120,
      get_pty=True)
    
    join = SSHOperator(
      task_id='join',
      ssh_conn_id='ssh_worker',
      command='/home/na/worker.sh join2 run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
      cmd_timeout=120,
      get_pty=True)
    
    bird = SSHOperator(
      task_id='bird',
      ssh_conn_id='ssh_worker',
      command='/home/na/worker.sh bird2 run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
      cmd_timeout=120,
      get_pty=True)
    
    push = PythonOperator(
      task_id='push',
      python_callable=push_casts,
      provide_context=True,
    )
    
    gambit >> join >> bird >> push
  return dag