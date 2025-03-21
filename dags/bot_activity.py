import datetime
import airflow
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from utils import pull_trending_casts


default_args = {
  'start_date': airflow.utils.dates.days_ago(1),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


def insert_trending_casts(**context):
  casts = pull_trending_casts()
  df = pd.DataFrame(casts)
  print('trending casts pulled: ', len(df))
  if df is None or len(df)==0:
    return
  pg_hook = PostgresHook(postgres_conn_id='pg_dsart')
  engine = pg_hook.get_sqlalchemy_engine()
  with engine.connect() as connection:
    df.to_sql(
      'tmp_trending_casts',
      engine,
      schema='public',
      if_exists='replace',
      index=False
    )
    print("Casts inserted into temporary table.")
    sql1 = """INSERT INTO ds.trending_casts 
              SELECT * FROM tmp_trending_casts WHERE hash NOT IN (SELECT hash FROM ds.trending_casts);"""
    connection.execute(sql1)
    print("Updated trending_casts table.")


with DAG(
  'bot_activity',
  default_args=default_args,
  description='dsart bot hourly activity',
  schedule_interval='30 * * * *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

  trending = PythonOperator(
    task_id='trending',
    python_callable=insert_trending_casts
  )

  publish = SSHOperator(
    task_id='publish',
    ssh_conn_id='ssh_worker',
    command='/home/na/worker.sh autopilot run   ',
    cmd_timeout=1200,
    get_pty=True)
  publish
  
  profiles = SSHOperator(
    task_id='profiles',
    ssh_conn_id='ssh_worker',
    command='/home/na/worker.sh bot_batch new_profiles 5   ',
    cmd_timeout=1800,
    get_pty=True)
  profiles
  
  trending >> publish >> profiles 

    