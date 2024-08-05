WITH cast_data AS (
    SELECT 
        timestamp::date AS day,
        EXTRACT(HOUR FROM timestamp) AS hour,
        encode(casts.hash, 'hex') AS cast_hash,
        deleted_at,
        (SELECT count(distinct fid) FROM messages 
         WHERE body->'target'->>'hash'=('0x'||encode(casts.hash, 'hex'))
         AND body->>'type'='1'
         AND timestamp < (casts.timestamp + INTERVAL '12 hours')) AS num_like,
        (SELECT count(distinct fid) FROM messages 
         WHERE body->'target'->>'hash'=('0x'||encode(casts.hash, 'hex'))
         AND body->>'type'='2'
         AND timestamp < (casts.timestamp + INTERVAL '12 hours')) AS num_recast,
        (SELECT count(distinct fid) FROM messages 
         WHERE body->'parent'->>'hash'=('0x'||encode(casts.hash, 'hex'))
         AND type = 1
         AND timestamp < (casts.timestamp + INTERVAL '12 hours')) AS num_reply
    FROM public.casts
    WHERE 
        timestamp>'{{ execution_date - macros.timedelta(hours=12) }}'
		AND timestamp<'{{ execution_date - macros.timedelta(hours=11) }}'
)

SELECT *
FROM cast_data
WHERE (deleted_at IS NOT NULL) OR (num_like > 0) OR (num_recast > 0) OR (num_reply > 0);



