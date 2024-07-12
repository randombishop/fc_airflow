import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'start_date': airflow.utils.dates.days_ago(3),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'daily_agg',
    default_args=default_args,
    description='Run daily data aggregations on PG database',
    schedule_interval='0 4 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

	daily_links_task = PostgresOperator(
		task_id='daily_links',
		postgres_conn_id='pg_replicator',
		sql='sql/daily_links.sql'
	)
	daily_casts_task = PostgresOperator(
		task_id='daily_casts',
		postgres_conn_id='pg_replicator',
		sql='sql/daily_casts.sql'
	)    
	daily_links_task >> daily_casts_task



