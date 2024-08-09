import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(3),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}

with DAG(
    'daily_agg',
    default_args=default_args,
    description='Run daily data aggregations on PG database',
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    links_query = PostgresOperator(
		task_id='links_query',
		postgres_conn_id='pg_replicator',
		sql='sql/daily_links.sql')
 
    links_snapshot = PostgresToGCSOperator(
        task_id="links_snapshot",
        postgres_conn_id='pg_replicator',
        sql='sql/links.sql',
        bucket='dsart_nearline1',
        filename='pipelines/snapshots/links/{{ ds }}.csv',
        export_format="csv",
        gzip=False)
    
    daily_casts = PostgresOperator(
		task_id='daily_casts',
		postgres_conn_id='pg_replicator',
		sql='sql/daily_casts.sql')
  
    daily_casts

    links_query >> links_snapshot
