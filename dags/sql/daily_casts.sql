INSERT INTO ds.daily_casts (day, num_cast, num_fid) (
	SELECT timestamp::timestamp::date as day, 
		   count(*) as num_cast,
		   count(distinct fid) as num_fid
	FROM public.casts 
	WHERE timestamp>'{{ ds }}' 
    AND timestamp<'{{ macros.ds_add(ds, 1) }}' 
	AND deleted_at IS NULL
	group by day 
	order by day
)