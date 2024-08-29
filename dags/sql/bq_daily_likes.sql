SELECT 
avg(h36_likes) as num_likes,
avg(h36_recasts) as num_recasts,
avg(h36_replies) as num_replies
FROM dsart_farcaster.cast_features
WHERE day = '{{ (execution_date - macros.timedelta(days=2)).strftime("%Y-%m-%d") }}'

