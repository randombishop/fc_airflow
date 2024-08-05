import datetime
import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


bq_fields = [
    {'name': 'day', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'hour', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
    {'name': 'cast_hash', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'deleted_at', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
    {'name': 'num_like', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
    {'name': 'num_recast', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
    {'name': 'num_reply', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
]

default_args = {
    'start_date': airflow.utils.dates.days_ago(75),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'etl_engagement',
    default_args=default_args,
    description='Hourly engagement stats',
    schedule_interval='0 * * * *',
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

    filename1 = 'pipelines/etl_engagement/snapshot_1h/{{ (execution_date - macros.timedelta(hours=1)).strftime("%Y-%m-%d-%H") }}_eng01.csv'

    etleng_sql1 = PostgresToGCSOperator(
        task_id="etleng_sql_01",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_1h.sql',
        bucket='dsart_nearline1',
        filename=filename1,
        export_format="csv",
        gzip=False
    )

    etleng_bq1 = GCSToBigQueryOperator(
        task_id='etleng_bq_01',
        bucket='dsart_nearline1',
        source_objects=[filename1],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_farcaster.engagement01h',
        time_partitioning={
            'type': 'DAY',
            'field': 'day', 
        },
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        source_format='CSV',
        schema_fields=bq_fields
    )

    filename12 = 'pipelines/etl_engagement/snapshot_12h/{{ (execution_date - macros.timedelta(hours=12)).strftime("%Y-%m-%d-%H") }}_eng12.csv'

    etleng_sql12 = PostgresToGCSOperator(
        task_id="etleng_sql_12",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_12h.sql',
        bucket='dsart_nearline1',
        filename=filename12,
        export_format="csv",
        gzip=False
    )

    etleng_bq12 = GCSToBigQueryOperator(
        task_id='etleng_bq_12',
        bucket='dsart_nearline1',
        source_objects=[filename12],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_farcaster.engagement12h',
        time_partitioning={
            'type': 'DAY',
            'field': 'day', 
        },
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        source_format='CSV',
        schema_fields=bq_fields
    )

    filename36 = 'pipelines/etl_engagement/snapshot_36h/{{  (execution_date - macros.timedelta(hours=36)).strftime("%Y-%m-%d-%H") }}_eng36.csv'

    etleng_sql36 = PostgresToGCSOperator(
        task_id="etleng_sql_36",
        postgres_conn_id='pg_replicator',
        sql='sql/engagement_36h.sql',
        bucket='dsart_nearline1',
        filename=filename36,
        export_format="csv",
        gzip=False
    )

    etleng_bq36 = GCSToBigQueryOperator(
        task_id='etleng_bq_36',
        bucket='dsart_nearline1',
        source_objects=[filename36],
        destination_project_dataset_table='deep-mark-425321-r7.dsart_farcaster.engagement36h',
        time_partitioning={
            'type': 'DAY',
            'field': 'day', 
        },
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        source_format='CSV',
        schema_fields=bq_fields
    )

    etleng_sql1 >> etleng_bq1
    etleng_sql12 >> etleng_bq12
    etleng_sql36 >> etleng_bq36



