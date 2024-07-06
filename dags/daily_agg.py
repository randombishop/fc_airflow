import datetime
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


SQL_DAILY_CASTS = """
INSERT INTO ds.daily_casts (day, num_cast, num_fid) (
	SELECT timestamp::timestamp::date as day, 
		   count(*) as num_cast,
		   count(distinct fid) as num_fid
	FROM public.casts 
	WHERE timestamp>'{{ ds }}' 
    AND timestamp<'{{ macros.ds_add(ds, 1) }}' 
	AND deleted_at IS NULL
	group by day 
	order by day
)

"""

SQL_DAILY_LINKS = """
INSERT INTO ds.daily_links (day, fid, link_from_add, link_from_del, link_to_add, link_to_del) (

	WITH msg AS (
		SELECT timestamp::timestamp::date as day,
			   fid, type, body FROM public.messages
		WHERE timestamp>'{{ ds }}' 
        AND timestamp<'{{ macros.ds_add(ds, 1) }}' 
        AND type in (5,6)
        AND deleted_at is NULL
        AND revoked_at is NULL
	),
	link_from AS (
		SELECT day, fid, 
		array_agg(distinct body->>'targetFid') filter (where type=5) as link_from_add ,
		array_agg(distinct body->>'targetFid') filter (where type=6) as link_from_del	
		FROM msg
		GROUP BY (day, fid)),
	link_to AS (
		SELECT day, (body->>'targetFid')::BIGINT as fid, 
		array_agg(distinct fid) filter (where type=5) as link_to_add ,
		array_agg(distinct fid) filter (where type=6) as link_to_del	
		FROM msg
		GROUP BY (day, body->>'targetFid'))
	
	SELECT 
		COALESCE(link_from.day, link_to.day) as day ,
		COALESCE(link_from.fid, link_to.fid) as fid ,
		link_from_add, link_from_del,
		link_to_add, link_to_del
	FROM 
		link_from FULL JOIN link_to 
		ON link_from.day=link_to.day and link_from.fid=link_to.fid  
	ORDER BY 
		day, fid
	
)
"""


default_args = {
    'start_date': airflow.utils.dates.days_ago(3),
    'retries': 1,
    'retry_delay': datetime.timedelta(hours=1)
}


with DAG(
    'daily_agg',
    default_args=default_args,
    description='Run daily data aggregations on PG database',
    schedule_interval='0 4 * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

	daily_links_task = PostgresOperator(
		task_id='daily_links',
		postgres_conn_id='pg_replicator',
		sql=SQL_DAILY_LINKS
	)
	daily_casts_task = PostgresOperator(
		task_id='daily_casts',
		postgres_conn_id='pg_replicator',
		sql=SQL_DAILY_CASTS
	)    
	daily_links_task >> daily_casts_task



