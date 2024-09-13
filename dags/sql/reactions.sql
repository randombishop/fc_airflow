SELECT 
	fid as fid,
  min(timestamp) as first_timestamp,
  max(timestamp) as last_timestamp,
	COALESCE(body->'target'->>'fid', body->'parent'->>'fid')::INTEGER as target_fid, 
	count(*) FILTER (WHERE type = 1) as num_replies,
	count(*) FILTER (WHERE type = 3 and body->>'type'='1') as num_likes,
	count(*) FILTER (WHERE type = 3 and body->>'type'='2') as num_recasts
FROM public.messages
WHERE timestamp>'{{ execution_date }}'
AND timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'
AND type in (1, 3)
AND COALESCE(body->'target'->>'fid', body->'parent'->>'fid') IS NOT NULL
AND COALESCE(body->'target'->>'fid', body->'parent'->>'fid')::BIGINT > 0
AND COALESCE(body->'target'->>'fid', body->'parent'->>'fid')::BIGINT < 100000000
GROUP BY fid, target_fid

