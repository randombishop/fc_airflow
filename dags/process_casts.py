import datetime
import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


default_args = {
    'start_date': airflow.utils.dates.days_ago(100),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'process_casts',
    default_args=default_args,
    description='Process casts hourly from postgres to BigQuery',
    schedule_interval='0 * * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(hours=3)
) as dag:

    snapshot_casts = PostgresToGCSOperator(
        task_id="snapshot_casts",
        postgres_conn_id='pg_replicator',
        sql='sql/snapshot_casts.sql',
        bucket='dsart_nearline1',
        filename='pipelines/process_cases/casts/{{ (execution_date - macros.timedelta(hours=9)).strftime("%Y-%m-%d-%H") }}_casts.csv',
        export_format="csv",
        gzip=False
    )

    snapshot_casts
