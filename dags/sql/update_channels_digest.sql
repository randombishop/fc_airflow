DROP TABLE IF EXISTS tmp_channel_activity ;

CREATE TEMP TABLE tmp_channel_activity AS (
	SELECT 
	body->>'parent' as channel, 
	count(*) as num_casts
	FROM public.messages
	WHERE timestamp>'{{ execution_date }}'
	AND timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'
	AND type=1
	AND json_typeof(body->'parent') = 'string' 
	GROUP BY channel 
	ORDER BY num_casts DESC	
) ;

UPDATE ds.channels_digest AS t
SET num_casts = t.num_casts + s.num_casts
FROM tmp_channel_activity s
WHERE t.url = s.channel ;

INSERT INTO ds.channels_digest (url, num_casts) (
	SELECT channel as url, num_casts FROM tmp_channel_activity
	WHERE channel not in (select url from ds.channels_digest)
) ;
