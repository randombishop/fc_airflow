import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'bot_activity',
    default_args=default_args,
    description='dsart bot hourly activity',
    schedule_interval='30 * * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=3)
) as dag:

    update1 = PostgresOperator(
		task_id='update1',
		postgres_conn_id='pg_replicator',
		sql='sql/update_channels_digest.sql')
    update1
    
    publish1 = SSHOperator(
        task_id='publish1',
        ssh_conn_id='ssh_worker',
        command='/home/na/channel_digest.sh    ',
        cmd_timeout=1200,
        get_pty=True)
    publish1
    
    update1 >> publish1

    