import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from dune_client.types import QueryParameter
from airflow.hooks.postgres_hook import PostgresHook
from utils import exec_dune_query
import logging

  
def call_dune(query_id, context):
  ts_from = context['logical_date'].strftime('%Y-%m-%d %H')+':00:00'
  ts_to = (context['logical_date'] + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H')+':00:00'
  logging.info(f"Calling Dune query {query_id} from {ts_from} to {ts_to}")
  params = [
    QueryParameter.text_type(name="ts_from", value=ts_from),
    QueryParameter.text_type(name="ts_to", value=ts_to)
  ]
  df = exec_dune_query(query_id, params)
  logging.info(f"Dataframe fetched from Dune: {len(df)}")
  return df


def update_channel_counts(**context):
  df = call_dune(4259101, context)
  pg_hook = PostgresHook(postgres_conn_id='pg_dsart')
  engine = pg_hook.get_sqlalchemy_engine()
  with engine.connect() as connection:
    df.to_sql('tmp_channel_activity', connection, if_exists='replace', index=False)
    logging.info(f"Uploaded to temp table tmp_channel_activity")
    sql1 = """UPDATE app.scheduled_action AS t
            SET count_casts = t.count_casts + s.num_casts
            FROM tmp_channel_activity s INNER JOIN ds.channels c ON s.channel = c.url
            WHERE t.count_channel = c.id ;"""
    connection.execute(sql1)
    logging.info(f"Executed SQL: {sql1}")
    sql2 = """INSERT INTO ds.channel_counts(channel, counted_at, num_casts) 
            SELECT c.id, now(), s.num_casts FROM tmp_channel_activity s 
            INNER JOIN ds.channels c ON s.channel = c.url ;"""
    connection.execute(sql2)
    logging.info(f"Executed SQL: {sql2}")
    sql_drop = "DROP TABLE tmp_channel_activity ;"
    connection.execute(sql_drop)
    logging.info(f"Dropped temp table tmp_channel_activity")
  logging.info(f"Done")
  

def update_category_counts(**context):
  df = call_dune(4340722, context)
  logging.info(df)
  pg_hook = PostgresHook(postgres_conn_id='pg_dsart')
  engine = pg_hook.get_sqlalchemy_engine()
  with engine.connect() as connection:
    df.to_sql('tmp_category_activity', connection, if_exists='replace', index=False)
    logging.info(f"Uploaded to temp table tmp_category_activity")
    sql1 = """UPDATE app.scheduled_action AS t
            SET count_casts = t.count_casts + s.num_casts
            FROM tmp_category_activity s
            WHERE t.count_category = s.category ;"""
    connection.execute(sql1)
    logging.info(f"Executed SQL: {sql1}")
    sql2 = """INSERT INTO ds.category_counts(category, counted_at, num_casts) 
            SELECT category, now(), num_casts FROM tmp_category_activity ;"""
    connection.execute(sql2)
    logging.info(f"Executed SQL: {sql2}")
    sql_drop = "DROP TABLE tmp_category_activity ;"
    connection.execute(sql_drop)
    logging.info(f"Dropped temp table tmp_category_activity")
  logging.info(f"Done")


def call_bot_engagement(context):
  query_id = 4562497
  ts_from = (context['logical_date'] - datetime.timedelta(hours=74)).strftime('%Y-%m-%d %H')+':00:00'
  ts_to = (context['logical_date'] - datetime.timedelta(hours=72)).strftime('%Y-%m-%d %H')+':00:00'
  ts_limit = context['logical_date'].strftime('%Y-%m-%d %H')+':00:00'
  fid = 788096
  logging.info(f"Calling Dune query {query_id} ts_from={ts_from} ts_to={ts_to} ts_limit={ts_limit} fid={fid}")
  params = [
    QueryParameter.text_type(name="ts_from", value=ts_from),
    QueryParameter.text_type(name="ts_to", value=ts_to),
    QueryParameter.text_type(name="ts_limit", value=ts_limit),
    QueryParameter.number_type(name="fid", value=fid)
  ]
  df = exec_dune_query(query_id, params)
  logging.info(f"Dataframe fetched from Dune: {len(df)}")
  return df


def update_bot_engagement(**context):
  df = call_bot_engagement(context)
  if df is None or len(df) == 0:
    logging.info(f"No bot engagement data fetched")
    return
  logging.info(df.head())
  pg_hook = PostgresHook(postgres_conn_id='pg_dsart')
  engine = pg_hook.get_sqlalchemy_engine()
  with engine.connect() as connection:
    df.to_sql('tmp_bot_engagement', connection, if_exists='replace', index=False)
    logging.info(f"Uploaded to temp table tmp_bot_engagement")
    sql1 = """UPDATE app.bot_cast AS t
            SET num_replies = s.num_replies,
                num_likes = s.num_likes,
                num_recasts = s.num_recasts
            FROM tmp_bot_engagement s
            WHERE t.cast_hash = s.hash ;"""
    connection.execute(sql1)
    logging.info(f"Executed SQL: {sql1}")
    sql_drop = "DROP TABLE tmp_bot_engagement ;"
    connection.execute(sql_drop)
    logging.info(f"Dropped temp table tmp_bot_engagement")
  logging.info(f"Done")



def create_task_group(dag):
  with TaskGroup(group_id='update_sched_tasks', dag=dag) as dag:
    update_channels = PythonOperator(
      task_id='update_channels',
      python_callable=update_channel_counts,
      provide_context=True,
    )
    update_categories = PythonOperator(
      task_id='update_categories',
      python_callable=update_category_counts,
      provide_context=True,
    )
    update_boteng = PythonOperator(
      task_id='update_boteng',
      python_callable=update_bot_engagement,
      provide_context=True,
    )
    update_channels >> update_categories >> update_boteng
  return dag
