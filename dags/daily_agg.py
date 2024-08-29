import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
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
  values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in to_insert.values()])
  insert_sql = f"""
  INSERT INTO dsart_farcaster.daily_stats ({columns})  
  VALUES ({values});
  """
  logging.info(f"Insert SQL: {insert_sql}")
  context['ti'].xcom_push(key='insert_sql1', value=insert_sql.strip())


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
  UPDATE dsart_farcaster.daily_stats 
  SET num_likes={result_1['num_likes']}, 
      num_replies={result_1['num_replies']}, 
      num_recasts={result_1['num_recasts']}, 
      spearman={result_2['spearman']} 
  WHERE day='{day}';
  """
  logging.info(f"Insert SQL: {insert_sql}")
  context['ti'].xcom_push(key='insert_sql2', value=insert_sql.strip())

    
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

  # Aggregate pg table casts to count daily volume
  daily_casts = PostgresOperator(
    task_id='daily_casts',
    postgres_conn_id='pg_replicator',
    sql='sql/daily_casts.sql')
  
  # pg->bq ETL to track followers and following
  links_filename = 'pipelines/daily_agg/links/{{ ds }}.csv'
  links_query = PostgresToGCSOperator(
    task_id='links_query',
    postgres_conn_id='pg_replicator',
    sql='sql/daily_links.sql',
    bucket='dsart_nearline1',
    filename=links_filename,
    export_format="csv",
    gzip=False)
  links_tmp = GCSToBigQueryOperator(
    task_id='links_tmp',
    bucket='dsart_nearline1',
    source_objects=[links_filename],
    destination_project_dataset_table='deep-mark-425321-r7.dsart_tmp.tmp_daily_links',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    source_format='CSV',
    autodetect=True
  )
  links_update = BigQueryExecuteQueryOperator(
    task_id='links_update',
    sql='sql/bq_links_update.sql',
    use_legacy_sql=False
  )
  links_snapshot_tmp = bq_stats = BigQueryExecuteQueryOperator(
    task_id='links_snapshot_tmp',
    sql='sql/bq_links_snapshot.sql',
    use_legacy_sql=False
  )
  links = BigQueryToGCSOperator(
    task_id="links",
    source_project_dataset_table='dsart_tmp.links_snapshot',
    destination_cloud_storage_uris=['gs://dsart_nearline1/pipelines/snapshots/links/{{ ds }}.csv'],
    export_format="csv")
  
  # Calculate daily stats and push them to daily_stats table
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
  bq_push1 = BigQueryExecuteQueryOperator(
    task_id='bq_push1',
    sql="{{ ti.xcom_pull(key='insert_sql1') }}",
    use_legacy_sql=False
  )
  bq_merge2 = PythonOperator(
    task_id='bq_merge2',
    python_callable=bq_merge2_function,
    provide_context=True,
  )
  bq_push2 = BigQueryExecuteQueryOperator(
    task_id='bq_push2',
    sql="{{ ti.xcom_pull(key='insert_sql2') }}",
    use_legacy_sql=False
  )
  
  daily_casts

  links_query >> links_tmp >> links_update >> links_snapshot_tmp >>links
  
  (bq_stats, bq_cats) >> bq_merge1 >> bq_push1
  
  (bq_likes, bq_corr) >> bq_merge2 >> bq_push2
