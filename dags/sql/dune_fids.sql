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

t_replies AS (
SELECT
parent_fid replies_fid,
COUNT(*) replies_num,
COUNT(CASE WHEN deleted_at is not null THEN 1 END) replies_del,
COUNT(DISTINCT fid) replies_ufid
FROM dune.neynar.dataset_farcaster_casts
WHERE timestamp>date_add('day', -30, current_date)
AND timestamp<current_date
AND parent_fid is not NULL
GROUP BY parent_fid
),

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
),

text1 AS (
    SELECT
        fid,
        LOWER(regexp_replace(text, 'https?://[^\s]+', '')) AS text_without_urls
    FROM dune.dsart.casts_features
        WHERE 
        timestamp>date_add('day', -30, current_date)
        and timestamp<current_date
        and text is not NULL
        and language is not NULL
),

text2 AS (
    SELECT
        fid,
        LOWER(regexp_replace(normalize(text_without_urls, NFD), '[^\p{L} ]', '')) AS cleaned_text
    FROM
        text1
),

tokenized AS (
    SELECT
        fid,
        keyword
    FROM
        text2, UNNEST(split(cleaned_text, ' ')) AS c(keyword)
    WHERE
        LENGTH(keyword) > 3
),

filtered_keywords AS (
    SELECT
        t.fid,
        t.keyword
    FROM
        tokenized t
    LEFT JOIN
        dune.dsart.dataset_ignore_keywords k ON t.keyword = k.word
    WHERE
        k.word IS NULL
),

keyword_counts AS (
    SELECT
        fid,
        keyword,
        COUNT(*) AS count
    FROM
        filtered_keywords
    GROUP BY
        fid,
        keyword
),

ranked_keywords AS (
    SELECT
        fid,
        keyword,
        count,
        ROW_NUMBER() OVER (PARTITION BY fid ORDER BY count DESC) AS rank
    FROM
        keyword_counts
),

t_keywords AS (
SELECT
    fid keywords_fid,
    map_agg(keyword, count) AS keywords
FROM
    ranked_keywords
WHERE
    rank <= 25
GROUP BY
    fid
),

prefs_rows AS (
SELECT
fid,
q_clear,q_audience,q_info,q_easy,q_verifiable,q_personal,q_funny,q_meme_ref,q_emo_res,q_happiness,q_curiosity,q_aggressivity,q_surprise,
q_interesting_ask,q_call_action,c_arts,c_business,c_crypto,c_culture,c_misc,c_money,c_na,c_nature,c_politics,c_sports,c_tech_science,
4 as weight
FROM dune.dsart.casts_features
WHERE timestamp>date_add('day', -30, current_date)
AND timestamp<current_date
AND (c_arts+c_business+c_crypto+c_culture+c_misc+c_money+c_na+c_nature+c_politics+c_sports+c_tech_science)>0  
UNION
SELECT 
r.fid,
q_clear,q_audience,q_info,q_easy,q_verifiable,q_personal,q_funny,q_meme_ref,q_emo_res,q_happiness,q_curiosity,q_aggressivity,q_surprise,
q_interesting_ask,q_call_action,c_arts,c_business,c_crypto,c_culture,c_misc,c_money,c_na,c_nature,c_politics,c_sports,c_tech_science,
(CASE WHEN reaction_type=1 THEN 2 ELSE 3 END) as weight
FROM dune.neynar.dataset_farcaster_reactions r
INNER JOIN dune.dsart.casts_features cf ON cf.hash=r.target_hash
WHERE r.timestamp>date_add('day', -30, current_date)
AND r.timestamp<current_date
AND (c_arts+c_business+c_crypto+c_culture+c_misc+c_money+c_na+c_nature+c_politics+c_sports+c_tech_science)>0
UNION
SELECT 
t1.fid,
t2.q_clear,t2.q_audience,t2.q_info,t2.q_easy,t2.q_verifiable,t2.q_personal,t2.q_funny,t2.q_meme_ref,t2.q_emo_res,t2.q_happiness,t2.q_curiosity,
t2.q_aggressivity,t2.q_surprise,t2.q_interesting_ask,t2.q_call_action,t2.c_arts,t2.c_business,t2.c_crypto,t2.c_culture,t2.c_misc,t2.c_money,
t2.c_na,t2.c_nature,t2.c_politics,t2.c_sports,t2.c_tech_science,
1 as weight
FROM dune.dsart.casts_features t1 
INNER JOIN dune.dsart.casts_features t2 ON t2.hash=t1.parent_hash 
WHERE t1.timestamp>date_add('day', -30, current_date)
AND t1.timestamp<current_date
AND (t2.c_arts+t2.c_business+t2.c_crypto+t2.c_culture+t2.c_misc+t2.c_money+t2.c_na+t2.c_nature+t2.c_politics+t2.c_sports+t2.c_tech_science)>0  
),

t_prefs AS (
SELECT
    fid prefs_fid,
    SUM(weight) AS prefs_weight,
    SUM(weight * q_clear) / SUM(weight) AS prefs_q_clear,
    SUM(weight * q_audience) / SUM(weight) AS prefs_q_audience,
    SUM(weight * q_info) / SUM(weight) AS prefs_q_info,
    SUM(weight * q_easy) / SUM(weight) AS prefs_q_easy,
    SUM(weight * q_verifiable) / SUM(weight) AS prefs_q_verifiable,
    SUM(weight * q_personal) / SUM(weight) AS prefs_q_personal,
    SUM(weight * q_funny) / SUM(weight) AS prefs_q_funny,
    SUM(weight * q_meme_ref) / SUM(weight) AS prefs_q_meme_ref,
    SUM(weight * q_emo_res) / SUM(weight) AS prefs_q_emo_res,
    SUM(weight * q_happiness) / SUM(weight) AS prefs_q_happiness,
    SUM(weight * q_curiosity) / SUM(weight) AS prefs_q_curiosity,
    SUM(weight * q_aggressivity) / SUM(weight) AS prefs_q_aggressivity,
    SUM(weight * q_surprise) / SUM(weight) AS prefs_q_surprise,
    SUM(weight * q_interesting_ask) / SUM(weight) AS prefs_q_interesting_ask,
    SUM(weight * q_call_action) / SUM(weight) AS prefs_q_call_action,
    SUM(weight * c_arts) / SUM(weight) AS prefs_c_arts,
    SUM(weight * c_business) / SUM(weight) AS prefs_c_business,
    SUM(weight * c_crypto) / SUM(weight) AS prefs_c_crypto,
    SUM(weight * c_culture) / SUM(weight) AS prefs_c_culture,
    SUM(weight * c_misc) / SUM(weight) AS prefs_c_misc,
    SUM(weight * c_money) / SUM(weight) AS prefs_c_money,
    SUM(weight * c_na) / SUM(weight) AS prefs_c_na,
    SUM(weight * c_nature) / SUM(weight) AS prefs_c_nature,
    SUM(weight * c_politics) / SUM(weight) AS prefs_c_politics,
    SUM(weight * c_sports) / SUM(weight) AS prefs_c_sports,
    SUM(weight * c_tech_science) / SUM(weight) AS prefs_c_tech_science
FROM
    prefs_rows
GROUP BY
    fid
),

ReplyTimes as (
SELECT
t1.fid,
LEAST(date_diff('second', t2.timestamp, t1.timestamp), 864000.0)/3600.0 delta
FROM dune.neynar.dataset_farcaster_casts t1
INNER JOIN dune.neynar.dataset_farcaster_casts t2 
ON t1.parent_hash=t2.hash
WHERE t1.timestamp>date_add('day', -30, current_date)
AND t1.timestamp<current_date
AND t1.parent_hash is not NULL
AND t1.deleted_at is NULL
),

t_time_reply AS (
SELECT 
fid time_reply_fid, 
CAST(AVG(delta) AS REAL) time_reply
FROM ReplyTimes
GROUP BY fid
),

ReactTimes as (
SELECT
t1.fid,
LEAST(date_diff('second', t2.timestamp, t1.timestamp), 864000.0)/3600.0 delta
FROM dune.neynar.dataset_farcaster_reactions t1
INNER JOIN dune.neynar.dataset_farcaster_casts t2 
ON t1.target_hash=t2.hash
WHERE t1.timestamp>date_add('day', -30, current_date)
AND t1.timestamp<current_date
AND t1.target_hash is not NULL
AND t1.deleted_at is NULL
),

t_time_react AS (
SELECT 
fid time_react_fid, 
CAST(AVG(delta) AS REAL) time_react
FROM ReactTimes
GROUP BY fid
),

t_features AS (
SELECT 
t_users.*, 
COALESCE((casts_30d_num>0 or react_out_num>0), FALSE) is_active,
t_casts_all.*, 
t_casts_30d.*,
CAST(casts_30d_del as REAL)/casts_30d_num as casts_30d_del_ratio,
t_channels.channels,
t_react_out.*,
CAST(react_out_del as REAL)/react_out_num as react_out_del_ratio,
t_react_in.*,
t_replies.*,
t_prefs.*,
t_lang.lang_1, 
t_lang.lang_2,
t_keywords.keywords,
t_time_reply.time_reply,
t_time_react.time_react,
COALESCE((time_reply+time_react)/2.0, COALESCE(time_reply, time_react)) as spam_time,
CAST(casts_30d_num as REAL)/casts_30d_active_days as spam_daily_casts,
COALESCE(
  ((CAST(casts_30d_del as REAL)/casts_30d_num)+(CAST(react_out_del as REAL)/react_out_num))/2.0, 
  COALESCE(CAST(casts_30d_del as REAL)/casts_30d_num, CAST(react_out_del as REAL)/react_out_num)
) as spam_delete_ratio
FROM t_users 
LEFT JOIN t_casts_all ON t_casts_all.casts_all_fid=t_users.fid
LEFT JOIN t_casts_30d ON t_casts_30d.casts_30d_fid=t_users.fid
LEFT JOIN t_channels ON t_channels.channels_fid=t_users.fid
LEFT JOIN t_react_out ON t_react_out.react_out_fid=t_users.fid
LEFT JOIN t_react_in ON t_react_in.react_in_fid=t_users.fid
LEFT JOIN t_replies ON t_replies.replies_fid=t_users.fid
LEFT JOIN t_prefs ON t_prefs.prefs_fid=t_users.fid
LEFT JOIN t_lang ON t_lang.lang_fid=t_users.fid
LEFT JOIN t_keywords ON t_keywords.keywords_fid=t_users.fid
LEFT JOIN t_time_reply ON t_time_reply.time_reply_fid=t_users.fid
LEFT JOIN t_time_react ON t_time_react.time_react_fid=t_users.fid 
),

t_stats AS (
SELECT 
approx_percentile(spam_time, 0.05) AS spam_time_05,
approx_percentile(spam_daily_casts, 0.95) AS spam_daily_casts_95,
approx_percentile(spam_delete_ratio, 0.95) AS spam_delete_ratio_95
FROM t_features
WHERE is_active=TRUE
),

t_spam_flags AS (
SELECT 
fid,
(spam_time<(SELECT spam_time_05 FROM t_stats)) spam_time_flag,
(spam_daily_casts>(SELECT spam_daily_casts_95 FROM t_stats)) spam_daily_casts_flag,
(spam_delete_ratio>(SELECT spam_delete_ratio_95 FROM t_stats)) spam_delete_ratio_flag
FROM t_features
)



SELECT
t_features.*,
spam_time_flag,
spam_daily_casts_flag,
spam_delete_ratio_flag,
(spam_time_flag=TRUE or spam_daily_casts_flag=TRUE or spam_delete_ratio_flag=TRUE) spam_any_flag
FROM t_features
LEFT JOIN t_spam_flags ON t_spam_flags.fid=t_features.fid
ORDER BY fid
