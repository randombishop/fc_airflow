SELECT 
	fid, 
	min(timestamp) as first_cast,
	max(timestamp) as last_cast,
	count(*) as num_casts,
	(SELECT DISTINCT ON (fid)
	    body->>'value'
        FROM messages
        WHERE fid=casts.fid 
        AND type = 11 
        AND body->>'type' = '6'
	    ORDER BY fid, timestamp DESC) as username
FROM public.casts
GROUP BY fid