SELECT (count(*)>1000) as check_new_casts 
FROM public.casts
WHERE timestamp>'{{ execution_date }}'
AND timestamp<'{{ execution_date + macros.timedelta(hours=1) }}'