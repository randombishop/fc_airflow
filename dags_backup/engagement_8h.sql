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
