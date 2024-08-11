SELECT 
count(*) as num_casts,
count(distinct fid) as num_fids,
avg(q_clear) as q_clear,
avg(q_audience) as q_audience,
avg(q_info) as q_info,
avg(q_easy) as q_easy,
avg(q_verifiable) as q_verifiable,
avg(q_personal) as q_personal,
avg(q_funny) as q_funny,
avg(q_meme_ref) as q_meme_ref,
avg(q_emo_res) as q_emo_res,
avg(q_happiness) as q_happiness,
avg(q_curiosity) as q_curiosity,
avg(q_aggressivity) as q_aggressivity,
avg(q_surprise) as q_surprise,
avg(q_interesting_ask) as q_interesting_ask,
avg(q_call_action) q_call_action,
avg(predict_like) as predict_like
FROM `deep-mark-425321-r7.dsart_farcaster.cast_features` 
WHERE day = '{{ ds }}'