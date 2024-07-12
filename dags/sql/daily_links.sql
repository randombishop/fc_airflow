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