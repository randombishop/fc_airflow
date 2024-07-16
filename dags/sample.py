import datetime
import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


SQL_SAMPLE_100K = """
SELECT id, encode(hash, 'hex') as hash, timestamp, fid, text
FROM casts
WHERE timestamp>'{{ ds }}' AND timestamp<'{{ macros.ds_add(ds, 1) }}' 
AND deleted_at is NULL
AND length(text)>=25
ORDER BY hash
LIMIT 100000
"""


default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'sample',
    default_args=default_args,
    description='Extract daily samples',
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

    sample100k_task = PostgresToGCSOperator(
        task_id="sample_100k",
        postgres_conn_id='pg_replicator',
        sql=SQL_SAMPLE_100K,
        bucket='dsart_nearline1',
        filename='pipelines/samples/100k/{{ ds }}_df.csv',
        export_format="csv",
        gzip=False
    )

    sample100k_task
