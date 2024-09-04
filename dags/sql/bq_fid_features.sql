CREATE OR REPLACE TABLE dsart_tmp.fid_features_params AS 
SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AS cutoff_date;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_msg AS 
SELECT 
fid,
count(*) msg_num_days,
sum(messages) as msg_messages,
sum(messages)/count(*) as msg_messages_per_day, 
sum(casts) as msg_casts,
sum(replies) as msg_replies,
sum(in_channels) as msg_casts_in_channels,
sum(delete_casts) as msg_delete_casts,
sum(likes) as msg_likes,
sum(recasts) as msg_recasts,
sum(delete_reactions) as msg_delete_reactions,
sum(follows) as msg_follows,
sum(delete_follows) as msg_delete_follows,
avg(avg_time_either) as msg_avg_time_any,
avg(avg_time_react)	as msg_avg_time_react,
avg(avg_time_reply) as msg_avg_time_reply,
sum(delete_casts+delete_reactions+delete_follows) as msg_total_deletes,
sum(delete_casts+delete_reactions+delete_follows)/sum(messages) as msg_ratio_deletes
FROM dsart_farcaster.fid_stats_messages
WHERE day>(SELECT cutoff_date FROM dsart_tmp.fid_features_params)
GROUP BY fid ;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_msg_stats AS 
SELECT
PERCENTILE_CONT(msg_messages_per_day, 0.95) OVER() AS threshold_messages_per_day,
PERCENTILE_CONT(msg_ratio_deletes, 0.95) OVER() AS threshold_ratio_deletes,
PERCENTILE_CONT(msg_avg_time_any, 0.05) OVER() AS threshold_avg_time
FROM dsart_tmp.fid_features_msg 
WHERE msg_messages>10 and msg_num_days>1
LIMIT 1;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_spam AS 
SELECT
fid,
IF(msg_messages>10 AND msg_num_days>1 AND msg_messages_per_day>(SELECT threshold_messages_per_day from dsart_tmp.fid_features_msg_stats),
1,0) AS spam_messages_per_day,
IF(msg_messages>10 AND msg_num_days>1 AND msg_ratio_deletes>(SELECT threshold_ratio_deletes from dsart_tmp.fid_features_msg_stats),
1,0) AS spam_deletes,
IF(msg_messages>10 AND msg_num_days>1 AND msg_avg_time_any<(SELECT threshold_avg_time from dsart_tmp.fid_features_msg_stats),
1,0) AS spam_speed
FROM dsart_tmp.fid_features_msg 
ORDER BY fid;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_followers AS 
SELECT 
fid_followed as fid,
count(*) as followers_num,
FROM dsart_farcaster.followers
WHERE (removed_at is null) or (removed_at<added_at)
GROUP BY fid_followed 
ORDER BY fid ;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_following AS 
SELECT 
fid_follower as fid,
count(*) as following_num,
FROM dsart_farcaster.followers
WHERE (removed_at is null) or (removed_at<added_at)
GROUP BY fid_follower 
ORDER BY fid;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_eng AS 
SELECT 
fid,
COUNT(*) AS eng_num_days,
SUM(likes_num) AS eng_likes,
SUM(recasts_num) AS eng_recasts,
SUM(replies_num) AS eng_replies,
AVG(SAFE_DIVIDE(likes_ufids,likes_num)) AS eng_ufids_likes_ratio,
AVG(SAFE_DIVIDE(recasts_ufids,recasts_num)) AS eng_ufids_recasts_ratio,
AVG(SAFE_DIVIDE(replies_ufids,replies_num)) AS eng_ufids_replies_ratio,
FROM dsart_farcaster.fid_stats_engagement
WHERE day>(SELECT cutoff_date FROM dsart_tmp.fid_features_params)
GROUP BY fid 
ORDER BY fid;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_prefs AS 
SELECT 
fid,
count(*) as prefs_num_days,
SUM(weight*q_clear)/SUM(weight) as prefs_q_clear, 
SUM(weight*q_audience)/SUM(weight) as prefs_q_audience, 
SUM(weight*q_info)/SUM(weight) as prefs_q_info, 
SUM(weight*q_easy)/SUM(weight) as prefs_q_easy, 
SUM(weight*q_verifiable)/SUM(weight) as prefs_q_verifiable, 
SUM(weight*q_personal)/SUM(weight) as prefs_q_personal, 
SUM(weight*q_funny)/SUM(weight) as prefs_q_funny, 
SUM(weight*q_meme_ref)/SUM(weight) as prefs_q_meme_ref, 
SUM(weight*q_emo_res)/SUM(weight) as prefs_q_emo_res, 
SUM(weight*q_happiness)/SUM(weight) as prefs_q_happiness, 
SUM(weight*q_curiosity)/SUM(weight) as prefs_q_curiosity, 
SUM(weight*q_aggressivity)/SUM(weight) as prefs_q_aggressivity, 
SUM(weight*q_surprise)/SUM(weight) as prefs_q_surprise, 
SUM(weight*q_interesting_ask)/SUM(weight) as prefs_q_interesting_ask, 
SUM(weight*q_call_action)/SUM(weight) as prefs_q_call_action, 
SUM(weight*c_arts)/SUM(weight) as prefs_c_arts, 
SUM(weight*c_business)/SUM(weight) as prefs_c_business, 
SUM(weight*c_crypto)/SUM(weight) as prefs_c_crypto, 
SUM(weight*c_culture)/SUM(weight) as prefs_c_culture, 
SUM(weight*c_misc)/SUM(weight) as prefs_c_misc, 
SUM(weight*c_money)/SUM(weight) as prefs_c_money, 
SUM(weight*c_na)/SUM(weight) as prefs_c_na, 
SUM(weight*c_nature)/SUM(weight) as prefs_c_nature, 
SUM(weight*c_politics)/SUM(weight) as prefs_c_politics, 
SUM(weight*c_sports)/SUM(weight) as prefs_c_sports, 
SUM(weight*c_tech_science)/SUM(weight) as prefs_c_tech_science, 
sum(weight) as prefs_total_weight
FROM dsart_farcaster.fid_stats_prefs
WHERE day>(SELECT cutoff_date FROM dsart_tmp.fid_features_params)
GROUP BY fid 
ORDER BY fid;


CREATE OR REPLACE TABLE dsart_tmp.fid_features_lang AS (
WITH 
lang_counts AS (
SELECT 
fid, 
language as lang, 
count(*) as freq
FROM dsart_farcaster.fid_stats_prefs
WHERE day>(SELECT cutoff_date FROM dsart_tmp.fid_features_params)
AND language is not null
group by fid, language
order by fid),
lang_ranks AS (
SELECT
fid,
lang,
freq,
ROW_NUMBER() OVER (PARTITION BY fid ORDER BY freq DESC) AS rn
FROM
lang_counts
ORDER BY freq DESC
) 
SELECT
fid,
MAX(CASE WHEN rn = 1 THEN lang END) AS lang_1,
MAX(CASE WHEN rn = 2 THEN lang END) AS lang_2,
FROM lang_ranks
WHERE rn<=2
GROUP BY fid
ORDER BY fid 
);


CREATE OR REPLACE TABLE dsart_tmp.fid_features_words AS (
WITH 
word_counts AS (
SELECT 
fid, 
word, 
count(*) as freq 
FROM dsart_farcaster.fid_stats_prefs, unnest(words) word 
WHERE day>(SELECT cutoff_date FROM dsart_tmp.fid_features_params)
group by fid, word
order by fid),
word_ranks AS (
SELECT
fid,
word,
freq,
ROW_NUMBER() OVER (PARTITION BY fid ORDER BY freq DESC) AS rn
FROM
word_counts
ORDER BY freq DESC
) 
SELECT
fid,
'{'||STRING_AGG('"'||word||'": '||freq, ', ')||'}' as words_dict
FROM word_ranks
WHERE rn<=50
GROUP BY fid
ORDER BY fid
) ;


CREATE OR REPLACE TABLE dsart_farcaster.fid_features
CLUSTER BY fid
AS
SELECT 
t.*,
msg.* EXCEPT(fid),
spam.* EXCEPT(fid),
f1.* EXCEPT(fid), 
f2.* EXCEPT(fid),
eng.* EXCEPT(fid),
prefs.* EXCEPT(fid), 
lang.* EXCEPT(fid), 
word.* EXCEPT(fid)
FROM dsart_farcaster.fid_username t
LEFT JOIN dsart_tmp.fid_features_msg msg ON t.fid=msg.fid
LEFT JOIN dsart_tmp.fid_features_spam spam ON t.fid=spam.fid
LEFT JOIN dsart_tmp.fid_features_followers f1 ON t.fid=f1.fid
LEFT JOIN dsart_tmp.fid_features_following f2 ON t.fid=f2.fid
LEFT JOIN dsart_tmp.fid_features_eng eng ON t.fid=eng.fid
LEFT JOIN dsart_tmp.fid_features_prefs prefs ON t.fid=prefs.fid
LEFT JOIN dsart_tmp.fid_features_lang lang ON t.fid=lang.fid
LEFT JOIN dsart_tmp.fid_features_words word ON t.fid=word.fid ;