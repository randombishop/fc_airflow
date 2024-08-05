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
    'etl_engagement',
    default_args=default_args,
    description='Hourly engagement stats',
    schedule_interval='0 * * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

    etleng_1h = PostgresToGCSOperator(
        task_id="etleng_1h",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_1h.sql',
        bucket='dsart_nearline1',
        filename='pipelines/etl_engagement/snapshot_1h/{{ (execution_date - macros.timedelta(hours=1)).strftime("%Y-%m-%d-%H") }}_eng01.csv',
        export_format="csv",
        gzip=False
    )

    etleng_12h = PostgresToGCSOperator(
        task_id="etleng_12h",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_12h.sql',
        bucket='dsart_nearline1',
        filename='pipelines/etl_engagement/snapshot_12h/{{ (execution_date - macros.timedelta(hours=12)).strftime("%Y-%m-%d-%H") }}_eng12.csv',
        export_format="csv",
        gzip=False
    )

    etleng_36h = PostgresToGCSOperator(
        task_id="etleng_36h",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_36h.sql',
        bucket='dsart_nearline1',
        filename='pipelines/etl_engagement/snapshot_36h/{{  (execution_date - macros.timedelta(hours=36)).strftime("%Y-%m-%d-%H") }}_eng36.csv',
        export_format="csv",
        gzip=False
    )

    etleng_1h >> etleng_12h >> etleng_36h



