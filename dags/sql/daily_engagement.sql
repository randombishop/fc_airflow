WITH msg AS (SELECT 
timestamp::timestamp::date AS day, 
CAST(COALESCE(t0.body->'parent'->>'fid', t0.body->'target'->>'fid') AS INTEGER) AS fid
t0.fid AS source_fid, 
t0.type AS type,
CASE WHEN t0.type=3 THEN CAST(t0.body->>'type' AS INTEGER) ELSE NULL END AS reaction_type,
FROM public.messages t0
WHERE t0.timestamp>'{{ ds }}'
AND t0.timestamp<'{{ macros.ds_add(ds, 1) }}' 
AND t0.type in (1, 3) 
AND ((t0.body->'parent'->>'fid' IS NOT NULL) OR (t0.body->'target'->>'fid' IS NOT NULL))

SELECT 
day,
fid,
COUNT(*) as reactions,
COUNT(*) FILTER (WHERE type=1) AS replies_num,
COUNT(*) FILTER (WHERE type=3 AND reaction_type=1) AS likes_num,
COUNT(*) FILTER (WHERE type=3 AND reaction_type=2) AS recasts_num,
COUNT(distinct source_fid) AS replies_ufids,
COUNT(distinct source_fid) FILTER (WHERE type=3 AND reaction_type=1) AS likes_ufids,
COUNT(distinct source_fid) FILTER (WHERE type=3 AND reaction_type=2) AS recasts_ufids,
array_agg(DISTINCT distinct source_fid) FILTER (WHERE type=1) AS replies_fids,
array_agg(DISTINCT distinct source_fid) FILTER (WHERE type=3 AND reaction_type=1) AS likes_fids,
array_agg(DISTINCT distinct source_fid) FILTER (WHERE type=3 AND reaction_type=2) AS recasts_fids,
FROM msg
GROUP BY day, fid
ORDER BY day, fid ASC
