import datetime
import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator




default_args = {
    'start_date': airflow.utils.dates.days_ago(35),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'snapshot',
    default_args=default_args,
    description='Run daily snapshots',
    schedule_interval='0 10 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=2)
) as dag:

    engagement8h_task = PostgresToGCSOperator(
        task_id="snap_engagement8h",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_8h.sql',
        bucket='dsart_nearline1',
        filename='pipelines/snapshots/engagement8h/{{ ds }}.csv',
        export_format="csv",
        gzip=False
    )

    links_task = PostgresToGCSOperator(
        task_id="snap_links",
        postgres_conn_id='pg_replicator',
        sql='sql/links.sql',
        bucket='dsart_nearline1',
        filename='pipelines/snapshots/links/{{ ds }}.csv',
        export_format="csv",
        gzip=False
    )

    engagement8h_task >> links_task



