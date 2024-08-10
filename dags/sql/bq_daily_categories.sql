SELECT 
category_label as category,
count(*) as num
FROM `deep-mark-425321-r7.dsart_farcaster.cast_features` 
WHERE day = '{{ ds }}'
GROUP BY category_label ;