SELECT 
category_label as category,
count(*) as num
FROM dsart_farcaster.cast_features
WHERE day = '{{ ds }}'
GROUP BY category_label ;