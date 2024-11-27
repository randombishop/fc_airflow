import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from dune_client.types import QueryParameter
from airflow.hooks.postgres_hook import PostgresHook
from utils import exec_dune_query
import logging

  

def update_channel_counts(**context):
  ts_from = context['logical_date'].strftime('%Y-%m-%d %H')+':00:00'
  ts_to = (context['logical_date'] + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H')+':00:00'
  logging.info(f"Pulling channel counts from {ts_from} to {ts_to}")
  params = [
    QueryParameter.text_type(name="ts_from", value=ts_from),
    QueryParameter.text_type(name="ts_to", value=ts_to)
  ]
  query_id = 4259101
  df = exec_dune_query(query_id, params)
  logging.info(f"Dataframe fetched from Dune: {len(df)}")
  pg_hook = PostgresHook(postgres_conn_id='pg_dsart')
  engine = pg_hook.get_sqlalchemy_engine()
  with engine.connect() as connection:
    df.to_sql('tmp_channel_activity', connection, if_exists='replace', index=False)
    logging.info(f"Uploaded to temp table tmp_channel_activity")
    sql1 = """UPDATE ds.channels_digest AS t
             SET num_casts = t.num_casts + s.num_casts
             FROM tmp_channel_activity s
             WHERE t.url = s.channel ;"""
    connection.execute(sql1)
    logging.info(f"Executed SQL: {sql1}")
    sql2 = """INSERT INTO ds.channels_digest (url, num_casts) (
            SELECT channel as url, num_casts FROM tmp_channel_activity
            WHERE channel not in (select url from ds.channels_digest)
          ) ;""" 
    connection.execute(sql2)
    logging.info(f"Executed SQL: {sql2}")
    sql3 = """UPDATE app.scheduled_action AS t
             SET num_casts = t.num_casts + s.num_casts
             FROM tmp_channel_activity s INNER JOIN ds.channels c ON s.url = c.url
             WHERE t.count_channel = c.id ;"""
    connection.execute(sql3)
    logging.info(f"Executed SQL: {sql3}")
    sql_drop = "DROP TABLE tmp_channel_activity ;"
    connection.execute(sql_drop)
    logging.info(f"Dropped temp table tmp_channel_activity")
  logging.info(f"Done")
  



default_args = {
  'start_date': airflow.utils.dates.days_ago(2),
  'retries': 1,
  'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
  'update_sched_tasks',
  default_args=default_args,
  description='Updates scheduled tasks cast counts',
  schedule_interval='55 */2 * * *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=datetime.timedelta(hours=1)
) as dag:

  update_channels = PythonOperator(
    task_id='update_channels',
    python_callable=update_channel_counts,
    provide_context=True,
  )
  
  update_channels