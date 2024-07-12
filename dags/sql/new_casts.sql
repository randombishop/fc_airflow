SELECT (count(*)>10) as check_new_casts 
FROM public.casts
WHERE timestamp>'{{ ts }}'