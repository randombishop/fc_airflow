INSERT INTO dsart_farcaster.fid_stats_engagement
SELECT 
day,
fid,
reactions,
replies_num,
likes_num,
recasts_num,
replies_ufids,
likes_ufids,
recasts_ufids,
CASE WHEN replies_fids IS NULL THEN NULL ELSE 
  ARRAY(SELECT CAST(value AS INT64) FROM UNNEST(SPLIT(REGEXP_REPLACE(replies_fids, r'\[|\]|\{|\}|\'', ''), ',')) AS value)
END AS replies_fids,
CASE WHEN likes_fids IS NULL THEN NULL ELSE 
  ARRAY(SELECT CAST(value AS INT64) FROM UNNEST(SPLIT(REGEXP_REPLACE(likes_fids, r'\[|\]|\{|\}|\'', ''), ',')) AS value)
END AS likes_fids,
CASE WHEN recasts_fids IS NULL THEN NULL ELSE 
  ARRAY(SELECT CAST(value AS INT64) FROM UNNEST(SPLIT(REGEXP_REPLACE(recasts_fids, r'\[|\]|\{|\}|\'', ''), ',')) AS value)
END AS recasts_fids
FROM 
  dsart_tmp.tmp_daily_engagement