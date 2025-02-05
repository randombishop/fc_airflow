import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from utils import pull_trending_casts


default_args = {
  'start_date': airflow.utils.dates.days_ago(1),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


def insert_trending_casts(**context):
  casts = pull_trending_casts()
  print(casts)


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

  publish1 = SSHOperator(
    task_id='publish1',
    ssh_conn_id='ssh_worker',
    command='/home/na/worker.sh scheduled_actions next   ',
    cmd_timeout=1200,
    get_pty=True)
  publish1
  
  trending >>publish1

    