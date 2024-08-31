WITH msg AS (SELECT 
timestamp::timestamp::date AS day, 
t0.fid AS fid, 
t0.type AS type,
CASE WHEN (t0.type=1) THEN t0.body->'parent'->>'hash' ELSE NULL END AS replied_to,
CASE WHEN ((t0.type=1) and (t0.body->'parent'->'hash' IS NULL)) THEN t0.body->>'parent' ELSE NULL END AS channel,
CASE WHEN t0.type=3 THEN CAST(t0.body->>'type' AS INTEGER) ELSE NULL END AS reaction_type,
CASE WHEN t0.type=3 THEN t0.body->'target'->>'hash' ELSE NULL END AS reacted_to,
EXTRACT(EPOCH FROM t0.timestamp) as message_ts,
(select EXTRACT(EPOCH FROM timestamp) from public.messages where hash=('\'||SUBSTRING(t0.body->'target'->>'hash' FROM 2))::bytea) AS reacted_to_ts,
(select EXTRACT(EPOCH FROM timestamp) from public.messages where hash=('\'||SUBSTRING(t0.body->'parent'->>'hash' FROM 2))::bytea) AS replied_to_ts
FROM public.messages t0
WHERE t0.timestamp>'{{ ds }}'
AND t0.timestamp<'{{ macros.ds_add(ds, 1) }}' 
)

SELECT 
day,
fid,
COUNT(*) as messages,
COUNT(*) FILTER (WHERE type=1) AS casts,
COUNT(replied_to) AS replies,
COUNT(channel) as in_channels,
COUNT(*) FILTER (WHERE type=2) AS delete_casts,
COUNT(*) FILTER (WHERE type=3 AND reaction_type=1) AS likes,
COUNT(*) FILTER (WHERE type=3 AND reaction_type=2) AS recasts,
COUNT(*) FILTER (WHERE type=4) AS delete_reactions,
COUNT(*) FILTER (WHERE type=5) AS follows,
COUNT(*) FILTER (WHERE type=6) AS delete_follows,
ROUND(AVG(message_ts - COALESCE(reacted_to_ts, replied_to_ts)),1) AS avg_time_either,
ROUND(AVG(message_ts - reacted_to_ts),1) AS avg_time_react,
ROUND(AVG(message_ts - replied_to_ts),1) AS avg_time_reply,
array_agg(DISTINCT replied_to) FILTER (WHERE replied_to IS NOT NULL) AS replied_to,
array_agg(DISTINCT reacted_to) FILTER (WHERE reacted_to IS NOT NULL AND reaction_type=1)  AS liked,
array_agg(DISTINCT reacted_to) FILTER (WHERE reacted_to IS NOT NULL AND reaction_type=2)  AS recasted,
array_agg(DISTINCT channel) FILTER (WHERE channel IS NOT NULL) AS channels
FROM msg
GROUP BY day, fid
ORDER BY day, fid ASC




