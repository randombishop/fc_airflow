SELECT encode(hash, 'hex') as hash, 
	timestamp,
	fid,
	body->>'text' as text,
	body->'embeds' as embeds,
	body->'mentions' as mentions,
	body->'mentionsPositions' as mentions_pos,
	
	CASE
	    WHEN json_typeof(body->'parent') = 'object' THEN (body->'parent'->>'fid')::int
	    ELSE NULL
	END AS parent_fid,
	
	CASE
	    WHEN json_typeof(body->'parent') = 'object' THEN body->'parent'->>'hash'
	    ELSE NULL
	END AS parent_hash,
	
	CASE
	    WHEN json_typeof(body->'parent') = 'string' THEN body->>'parent'
	    ELSE NULL
	END AS parent_url,
	
	(body->>'type')::int as body_type
	
FROM public.messages
WHERE 
	timestamp>'{{ execution_date - macros.timedelta(hours=9) }}'
	AND timestamp<'{{ execution_date - macros.timedelta(hours=8) }}'
	AND type=1