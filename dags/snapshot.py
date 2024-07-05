import datetime
import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


SQL_ENGAGEMENT_8H = """
SELECT id, 
	
	encode(casts.hash, 'hex') as cast_hash,
	
	(select count(distinct fid) from messages 
	 where body->'target'->>'hash'=('0x'||encode(casts.hash, 'hex'))
	 and body->>'type'='1'
     and timestamp<(casts.timestamp+INTERVAL '8 hours')) as num_like,

	(select count(distinct fid) from messages 
	 where body->'target'->>'hash'=('0x'||encode(casts.hash, 'hex'))
	 and body->>'type'='2'
     and timestamp<(casts.timestamp+INTERVAL '8 hours')) as num_recast,

	(select count(distinct fid) from messages 
	 where body->'parent'->>'hash'=('0x'||encode(casts.hash, 'hex'))
	 and type=1
     and timestamp<(casts.timestamp+INTERVAL '8 hours')) as num_reply
	
FROM public.casts

WHERE 
timestamp>'{{ ds }}' 
and timestamp<'{{ macros.ds_add(ds, 1) }}'
and deleted_at is NULL

"""


SQL_LINKS = """
SELECT fid,
	
  (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t1, 
   LATERAL unnest(link_from_add) e
   WHERE t1.fid=t0.fid 
   AND t1.day<'{{ macros.ds_add(ds, 1) }}'
   AND t1.link_from_add is not NULL) as link_from_add,

   (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t2, 
   LATERAL unnest(link_from_del) e
   WHERE t2.fid=t0.fid 
   AND t2.day<'{{ macros.ds_add(ds, 1) }}'
   AND t2.link_from_del is not NULL) as link_from_del,

   (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t3, 
   LATERAL unnest(link_to_add) e
   WHERE t3.fid=t0.fid 
   AND t3.day<'{{ macros.ds_add(ds, 1) }}'
   AND t3.link_to_add is not NULL) as link_to_add,

   (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t4, 
   LATERAL unnest(link_to_del) e
   WHERE t4.fid=t0.fid 
   AND t4.day<'{{ macros.ds_add(ds, 1) }}'
   AND t4.link_to_del is not NULL) as link_to_del
	
FROM 
(select distinct fid from ds.daily_links) as t0
"""


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
    catchup=True,
    dagrun_timeout=datetime.timedelta(hours=2)
) as dag:

    engagement8h_task = PostgresToGCSOperator(
        task_id="engagement8h",
        postgres_conn_id='pg_replicator',
        sql=SQL_ENGAGEMENT_8H,
        bucket='dsart_nearline1',
        filename='pipelines/snapshots/engagement8h/{{ ds }}.csv',
        export_format="csv",
        gzip=False
    )

    links_task = PostgresToGCSOperator(
        task_id="links",
        postgres_conn_id='pg_replicator',
        sql=SQL_LINKS,
        bucket='dsart_nearline1',
        filename='pipelines/snapshots/links/{{ ds }}.csv',
        export_format="csv",
        gzip=False
    )

    engagement8h_task >> links_task



