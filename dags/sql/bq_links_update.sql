INSERT INTO dsart_farcaster.fid_stats_links
SELECT 
  day,
  fid,
  
  CASE
    WHEN link_from_add IS NULL THEN NULL
    ELSE 
      ARRAY(
        SELECT CAST(value AS INT64)
        FROM UNNEST(SPLIT(REGEXP_REPLACE(link_from_add, r'\[|\]', ''), ',')) AS value
      )
    END AS link_from_add,

  CASE
    WHEN link_from_del IS NULL THEN NULL
    ELSE 
      ARRAY(
        SELECT CAST(value AS INT64)
        FROM UNNEST(SPLIT(REGEXP_REPLACE(link_from_del, r'\[|\]', ''), ',')) AS value
      )
    END AS link_from_del,

  CASE
    WHEN link_to_add IS NULL THEN NULL
    ELSE 
      ARRAY(
        SELECT CAST(value AS INT64)
        FROM UNNEST(SPLIT(REGEXP_REPLACE(link_to_add, r'\[|\]', ''), ',')) AS value
      )
    END AS link_to_add,

  CASE
    WHEN link_to_del IS NULL THEN NULL
    ELSE 
      ARRAY(
        SELECT CAST(value AS INT64)
        FROM UNNEST(SPLIT(REGEXP_REPLACE(link_to_del, r'\[|\]', ''), ',')) AS value
      )
    END AS link_to_del 

FROM 
  dsart_tmp.tmp_daily_links