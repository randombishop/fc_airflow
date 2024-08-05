SELECT id, 
	
	timestamp::date AS day,
    EXTRACT(HOUR FROM timestamp) AS hour,
	encode(casts.hash, 'hex') as cast_hash,
	deleted_at,
	
	(select count(distinct fid) from messages 
	 where body->'target'->>'hash'=('0x'||encode(casts.hash, 'hex'))
	 and body->>'type'='1'
     and timestamp<(casts.timestamp+INTERVAL '1 hours')) as num_like,

	(select count(distinct fid) from messages 
	 where body->'target'->>'hash'=('0x'||encode(casts.hash, 'hex'))
	 and body->>'type'='2'
     and timestamp<(casts.timestamp+INTERVAL '1 hours')) as num_recast,

	(select count(distinct fid) from messages 
	 where body->'parent'->>'hash'=('0x'||encode(casts.hash, 'hex'))
	 and type=1
     and timestamp<(casts.timestamp+INTERVAL '1 hours')) as num_reply
	
FROM public.casts

WHERE 
timestamp>'{{ execution_date - macros.timedelta(hours=1) }}'
AND timestamp<'{{ execution_date }}'
AND (deleted_at is not NULL or num_like>0 or num_recast>0 or num_reply>0) deleted_at is NULL
