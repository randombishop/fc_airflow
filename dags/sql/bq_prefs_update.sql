INSERT INTO dsart_farcaster.fid_stats_prefs
SELECT 
fid,
day,
weight,
q_clear,
q_audience,
q_info,
q_easy,
q_verifiable,
q_personal,
q_funny,
q_meme_ref,
q_emo_res,
q_happiness,
q_curiosity,
q_aggressivity,
q_surprise,
q_interesting_ask,
q_call_action,
c_arts,
c_business,
c_crypto,
c_culture,
c_misc,
c_money,
c_na,
c_nature,
c_politics,
c_sports,
c_tech_science,
language,
CASE WHEN words IS NULL THEN NULL ELSE 
  ARRAY(SELECT TRIM(value) FROM UNNEST(SPLIT(REGEXP_REPLACE(words, r'\[|\]|\{|\}|\'', ''), ',')) AS value)
END AS words
FROM 
  dsart_tmp.tmp_daily_prefs