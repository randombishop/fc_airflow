import datetime
import airflow
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

  
default_args = {
  'start_date': airflow.utils.dates.days_ago(2),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
  'casts_labels',
  default_args=default_args,
  description='Compute labels for casts using LLM',
  schedule_interval='59 */2 * * *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

  labels = SSHOperator(
    task_id='labels',
    ssh_conn_id='ssh_worker',
    command='/home/na/worker.sh labels3 run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
    cmd_timeout=120,
    get_pty=True)
  
  labels