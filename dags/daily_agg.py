import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
import os
import logging


def assemble_results(**context):
    job1 = context['ti'].xcom_pull(task_ids='bq_stats', key='job_id_path')
    logging.info(f"job1: {job1}")
    job2 = context['ti'].xcom_pull(task_ids='bq_cats', key='job_id_path')
    logging.info(f"job2: {job2}")
    job3 = context['ti'].xcom_pull(task_ids='bq_corr', key='job_id_path')
    logging.info(f"job3: {job3}")
    region = os.environ['EXECUTOR_REGION']
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    result_1 = bq_hook.get_query_results(job_id=job1, location=region)
    result_2 = bq_hook.get_query_results(job_id=job2, location=region)
    result_3 = bq_hook.get_query_results(job_id=job3, location=region)
    logging.info(f"result_1: {result_1}")
    logging.info(f"result_2: {result_2}")
    logging.info(f"result_3: {result_3}")
    to_insert = result_1.copy()
    for row in result_2:
        category = row['category']
        num = row['num']
        to_insert[category] = num
    to_insert['spearman'] = result_3
    logging.info(f"Data to insert in postgres: {to_insert}")
    columns = ', '.join(to_insert.keys())
    values = ', '.join([f"'{v}'" for v in to_insert.values()])
    insert_sql = f"""
    INSERT INTO target_table ({columns})
    VALUES ({values});
    """
    logging.info(f"Insert SQL: {insert_sql}")
    context['ti'].xcom_push(key='insert_sql', value=insert_sql)
    
    
default_args = {
    'start_date': airflow.utils.dates.days_ago(3),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'daily_agg',
    default_args=default_args,
    description='Run daily data aggregations on PG database',
    schedule_interval='0 2 * * *',
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
  
    bq_stats = BigQueryExecuteQueryOperator(
        task_id='bq_stats',
        sql='sql/bq_daily_stats.sql',
        use_legacy_sql=False
    )
    
    bq_cats = BigQueryExecuteQueryOperator(
        task_id='bq_cats',
        sql='sql/bq_daily_categories.sql',
        use_legacy_sql=False
    )
    
    bq_corr = BigQueryExecuteQueryOperator(
        task_id='bq_corr',
        sql='sql/bq_daily_correlation.sql',
        use_legacy_sql=False
    )
    
    bq_merge = PythonOperator(
        task_id='bq_merge',
        python_callable=assemble_results,
        provide_context=True,
    )
    
    bq_to_pg = PostgresOperator(
        task_id='bq_to_pg',
        postgres_conn_id='pg_replicator',
        sql="{{ ti.xcom_pull(key='insert_sql') }}",
    )
    
    daily_casts

    links_query >> links_snapshot
    
    (bq_stats, bq_cats, bq_corr) >> bq_merge >> bq_to_pg
