import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator



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

    update_cd = PostgresOperator(
		task_id='update_ca',
		postgres_conn_id='pg_replicator',
		sql='sql/update_channel_digest.sql')
    
    update_cd

    