import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
import logging


def assemble_results(**context):
    xcom1 = context['ti'].xcom_pull(task_ids='bq_stats')
    logging.info(f"XCOM1: {xcom1}")
    xcom2 = context['ti'].xcom_pull(task_ids='bq_cats')
    logging.info(f"XCOM2: {xcom2}")
    xcom3 = context['ti'].xcom_pull(task_ids='bq_corr')
    logging.info(f"XCOM3: {xcom3}")
    result_1 = xcom1[0]  # Single row, fields match target
    result_2 = xcom2  # List of rows with 'category' and 'num'
    result_3 = xcom3[0][0]  # Single number
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
        use_legacy_sql=False,
        xcom_push=True
    )
    
    bq_cats = BigQueryExecuteQueryOperator(
        task_id='bq_cats',
        sql='sql/bq_daily_categories.sql',
        use_legacy_sql=False,
        xcom_push=True
    )
    
    bq_corr = BigQueryExecuteQueryOperator(
        task_id='bq_corr',
        sql='sql/bq_daily_correlation.sql',
        use_legacy_sql=False,
        xcom_push=True
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
