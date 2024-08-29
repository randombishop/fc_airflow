WITH ranked_data AS (
  SELECT
    RANK() OVER (ORDER BY (IFNULL(h36_likes, 0)+IFNULL(h36_recasts, 0)+IFNULL(h36_replies, 0))) AS rank1,
    RANK() OVER (ORDER BY predict_like) AS rank2
  FROM  dsart_farcaster.cast_features
  WHERE day = '{{ (execution_date - macros.timedelta(days=2)).strftime("%Y-%m-%d") }}'
)
SELECT
  CORR(rank1, rank2) AS spearman
FROM
  ranked_data