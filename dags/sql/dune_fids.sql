WITH 

t_users AS (
SELECT
fid,
registered_at user_registered_at,
custody_address user_address,
(SELECT value FROM dune.neynar.dataset_farcaster_user_data 
WHERE fid=dataset_farcaster_fids.fid AND type=6 
ORDER BY timestamp desc LIMIT 1) as user_name,
(SELECT value FROM dune.neynar.dataset_farcaster_user_data 
WHERE fid=dataset_farcaster_fids.fid AND type=1 
ORDER BY timestamp desc LIMIT 1) as user_pfp,
(SELECT count(distinct fid) from dune.neynar.dataset_farcaster_links 
where target_fid=dataset_farcaster_fids.fid and type='follow' and deleted_at IS NULL) as follower_num,
(SELECT count(distinct target_fid) from dune.neynar.dataset_farcaster_links 
where fid=dataset_farcaster_fids.fid and type='follow' and deleted_at IS NULL) as following_num
FROM dune.neynar.dataset_farcaster_fids 
),

t_casts_all AS (
SELECT
fid casts_all_fid,
MIN(timestamp) casts_all_first,
MAX(timestamp) casts_all_last,
COUNT(*) casts_all_num,
COUNT(CASE WHEN deleted_at is not null THEN 1 END) casts_all_del
FROM dune.neynar.dataset_farcaster_casts
GROUP BY fid
),

t_casts_30d AS (
SELECT
fid casts_30d_fid,
COUNT(DISTINCT CAST(timestamp AS DATE)) casts_30d_active_days,
COUNT(*) casts_30d_num,
COUNT(CASE WHEN parent_url is not null THEN 1 END) casts_30d_num_in_channels,
COUNT(CASE WHEN parent_hash is not null THEN 1 END) casts_30d_replies,
COUNT(CASE WHEN deleted_at is not null THEN 1 END) casts_30d_del
FROM dune.neynar.dataset_farcaster_casts
WHERE 
timestamp>date_add('day', -30, current_date)
and timestamp<current_date
GROUP BY fid
),

t_channels AS (
SELECT 
    fid channels_fid,
    ARRAY_JOIN(ARRAY_AGG(channel ORDER BY frequency DESC), ', ') AS channels
FROM (
    SELECT 
        fid,
        channels.id as channel,
        COUNT(*) AS frequency,
        ROW_NUMBER() OVER (PARTITION BY fid ORDER BY COUNT(*) DESC) AS rank
    FROM 
        dune.neynar.dataset_farcaster_casts INNER JOIN dune.dsart.channels ON channels.url=dataset_farcaster_casts.parent_url
    WHERE
        dataset_farcaster_casts.timestamp>date_add('day', -30, current_date)
        and dataset_farcaster_casts.timestamp<current_date
    GROUP BY 
        dataset_farcaster_casts.fid, channels.id
) AS ranked_channels
WHERE rank <= 10
GROUP BY fid
ORDER BY fid
),

t_react_out AS
(SELECT
fid react_out_fid,
COUNT(*) react_out_num,
COUNT(CASE WHEN deleted_at is not null THEN 1 END) react_out_del,
COUNT(CASE WHEN reaction_type=1 THEN 1 END) react_out_likes,
COUNT(CASE WHEN reaction_type=2 THEN 1 END) react_out_recasts,
COUNT(DISTINCT target_fid) react_out_ufid
FROM dune.neynar.dataset_farcaster_reactions
WHERE timestamp>date_add('day', -30, current_date)
and timestamp<current_date
GROUP BY fid),

t_react_in AS
(SELECT
target_fid react_in_fid,
COUNT(*) react_in_num,
COUNT(CASE WHEN deleted_at is not null THEN 1 END) react_in_del,
COUNT(CASE WHEN reaction_type=1 THEN 1 END) react_in_likes,
COUNT(CASE WHEN reaction_type=2 THEN 1 END) react_in_recasts,
COUNT(DISTINCT fid) react_in_ufid
FROM dune.neynar.dataset_farcaster_reactions
WHERE timestamp>date_add('day', -30, current_date)
and timestamp<current_date
GROUP BY target_fid),

LanguageCounts AS (
  SELECT
      fid,
      language,
      COUNT(*) AS lang_count
  FROM
      dune.dsart.casts_features
  WHERE
      timestamp>date_add('day', -30, current_date)
      and timestamp<current_date
      and language IS NOT NULL
  GROUP BY
      fid, language
),

RankedLanguages AS (
    SELECT
        fid,
        language,
        lang_count,
        ROW_NUMBER() OVER (PARTITION BY fid ORDER BY lang_count DESC) AS lang_rank
    FROM
        LanguageCounts
),

t_lang AS (
SELECT
    fid lang_fid,
    MAX(CASE WHEN lang_rank = 1 THEN language END) AS lang_1,
    MAX(CASE WHEN lang_rank = 2 THEN language END) AS lang_2
FROM
    RankedLanguages
GROUP BY
    fid
)

SELECT 
t_users.*, 
t_casts_all.*, 
t_casts_30d.*,
t_channels.*,
t_react_out.*,
t_react_in.*,
t_lang.*
FROM t_users 
LEFT JOIN t_casts_all ON t_casts_all.casts_all_fid=t_users.fid
LEFT JOIN t_casts_30d ON t_casts_30d.casts_30d_fid=t_users.fid
LEFT JOIN t_channels ON t_channels.channels_fid=t_users.fid
LEFT JOIN t_react_out ON t_react_out.react_out_fid=t_users.fid
LEFT JOIN t_react_in ON t_react_in.react_in_fid=t_users.fid
LEFT JOIN t_lang ON t_lang.lang_fid=t_users.fid
ORDER BY fid
LIMIT 10 ;

