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


def bq_merge1_function(**context):
    xcom1 = context['ti'].xcom_pull(task_ids='bq_stats', key='job_id_path')
    xcom2 = context['ti'].xcom_pull(task_ids='bq_cats', key='job_id_path')
    region = os.environ['EXECUTOR_REGION']
    _, _, job1 = xcom1.split(':', 2)
    _, _, job2 = xcom2.split(':', 2)
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    result_1 = bq_hook.get_query_results(job_id=job1, location=region)
    result_2 = bq_hook.get_query_results(job_id=job2, location=region)
    logging.info(f"result_1: {result_1}")
    logging.info(f"result_2: {result_2}")
    to_insert = result_1[0].copy()
    to_insert['day'] = context['ds']
    for row in result_2:
        category = row['category']
        num = row['num']
        to_insert[category] = num
    logging.info(f"Data to insert in postgres: {to_insert}")
    columns = ', '.join(to_insert.keys())
    values = ', '.join([f"'{v}'" for v in to_insert.values()])
    insert_sql = f"""
    INSERT INTO ds.daily_stats ({columns})
    VALUES ({values});
    """
    logging.info(f"Insert SQL: {insert_sql}")
    context['ti'].xcom_push(key='insert_sql1', value=insert_sql)


def bq_merge2_function(**context):
    xcom1 = context['ti'].xcom_pull(task_ids='bq_likes', key='job_id_path')
    xcom2 = context['ti'].xcom_pull(task_ids='bq_corr', key='job_id_path')
    region = os.environ['EXECUTOR_REGION']
    _, _, job1 = xcom1.split(':', 2)
    _, _, job2 = xcom2.split(':', 2)
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    result_1 = bq_hook.get_query_results(job_id=job1, location=region)[0]
    result_2 = bq_hook.get_query_results(job_id=job2, location=region)[0]
    day = (context['execution_date'] - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    logging.info(f"result_1: {result_1}")
    logging.info(f"result_2: {result_2}")
    insert_sql = f"""
    UPDATE ds.daily_stats
    SET num_likes={result_1['num_likes']},
        num_replies={result_1['num_replies']},
        num_recasts={result_1['num_recasts']},
        spearman={result_2['spearman']}
    WHERE day='{day}';
    """
    logging.info(f"Insert SQL: {insert_sql}")
    context['ti'].xcom_push(key='insert_sql2', value=insert_sql)

    
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
    
    bq_likes = BigQueryExecuteQueryOperator(
        task_id='bq_likes',
        sql='sql/bq_daily_likes.sql',
        use_legacy_sql=False
    )
    
    bq_merge1 = PythonOperator(
        task_id='bq_merge1',
        python_callable=bq_merge1_function,
        provide_context=True,
    )
    
    bq_to_pg1 = PostgresOperator(
        task_id='bq_to_pg1',
        postgres_conn_id='pg_replicator',
        sql="{{ ti.xcom_pull(key='insert_sql1') }}",
    )
    
    bq_merge2 = PythonOperator(
        task_id='bq_merge2',
        python_callable=bq_merge2_function,
        provide_context=True,
    )
    
    bq_to_pg2 = PostgresOperator(
        task_id='bq_to_pg2',
        postgres_conn_id='pg_replicator',
        sql="{{ ti.xcom_pull(key='insert_sql2') }}",
    )
    
    daily_casts

    links_query >> links_snapshot
    
    (bq_stats, bq_cats) >> bq_merge1 >> bq_to_pg1
    
    (bq_likes, bq_corr) >> bq_merge2 >> bq_to_pg2
