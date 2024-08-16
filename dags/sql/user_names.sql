SELECT 
	main.fid, 
	min(main.timestamp) FILTER (WHERE type = 1) as first_cast,
	max(main.timestamp) FILTER (WHERE type = 1) as last_cast,
	COUNT(main.id) FILTER (WHERE type = 1) as num_casts,
	(
		SELECT body->>'value' 
		FROM public.messages AS sub
		WHERE sub.fid = main.fid 
			AND sub.type = 11 
			AND sub.body->>'type' = '6'
		ORDER BY sub.timestamp DESC
		LIMIT 1
	) AS user_name
FROM public.messages as main
WHERE main.timestamp>'{{ execution_date }}'
AND main.timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'
GROUP BY main.fid

