import airflow
from airflow import DAG
from airflow.operators.sql import SQLCheckOperator
from datetime import timedelta


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'check',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='0 * * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    task1 = SQLCheckOperator(
        task_id='check_casts',
        conn_id='pg_replicator',
		sql='sql/new_casts.sql')
    
    task1

