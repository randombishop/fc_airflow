SELECT 
	fid, 
	count(*) as num_casts_same_hour,
	(SELECT DISTINCT ON (fid)
	    body->>'value'
        FROM messages
        WHERE fid=casts.fid 
        AND type = 11 
        AND body->>'type' = '6'
	    AND timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'
	    ORDER BY fid, timestamp DESC) as user_name
FROM public.casts
WHERE timestamp>'{{ execution_date }}'
AND timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'
GROUP BY fid;