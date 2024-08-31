INSERT INTO dsart_farcaster.fid_stats_messages 
SELECT 
day,
fid,
messages,
casts,
replies,
in_channels,
delete_casts,
likes,
recasts,
delete_reactions,
follows,
delete_follows,
avg_time_either,
avg_time_react,
avg_time_reply,
CASE WHEN replied_to IS NULL THEN NULL ELSE 
  ARRAY(SELECT value FROM UNNEST(SPLIT(REGEXP_REPLACE(replied_to, r'\[|\]|\{|\}|\'', ''), ',')) AS value)
END AS replied_to,
CASE WHEN liked IS NULL THEN NULL ELSE 
  ARRAY(SELECT value FROM UNNEST(SPLIT(REGEXP_REPLACE(liked, r'\[|\]|\{|\}|\'', ''), ',')) AS value)
END AS liked,
CASE WHEN recasted IS NULL THEN NULL ELSE 
  ARRAY(SELECT value FROM UNNEST(SPLIT(REGEXP_REPLACE(recasted, r'\[|\]|\{|\}|\'', ',')) AS value)
END AS recasted,
CASE WHEN channels IS NULL THEN NULL ELSE 
  ARRAY(SELECT value FROM UNNEST(SPLIT(REGEXP_REPLACE(channels, r'\[|\]|\{|\}|\'', ',')) AS value)
END AS channels
FROM 
  dsart_tmp.tmp_daily_messages