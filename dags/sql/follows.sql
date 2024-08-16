SELECT 
	fid as fid_follower,
	(body->>'targetFid')::INTEGER as fid_followed, 
	max(timestamp) FILTER (WHERE type = 5) as added_at,
	max(timestamp) FILTER (WHERE type = 6) as removed_at
FROM public.messages
WHERE timestamp>'{{ execution_date }}'
AND timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'
AND type in (5, 6)
AND (body->>'targetFid')::BIGINT > 0
AND (body->>'targetFid')::BIGINT < 100000000
GROUP BY fid_follower, fid_followed

