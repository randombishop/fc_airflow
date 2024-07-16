SELECT fid,
	
  (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t1, 
   LATERAL unnest(link_from_add) e
   WHERE t1.fid=t0.fid 
   AND t1.day<'{{ macros.ds_add(ds, 1) }}'
   AND t1.link_from_add is not NULL) as link_from_add,

   (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t2, 
   LATERAL unnest(link_from_del) e
   WHERE t2.fid=t0.fid 
   AND t2.day<'{{ macros.ds_add(ds, 1) }}'
   AND t2.link_from_del is not NULL) as link_from_del,

   (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t3, 
   LATERAL unnest(link_to_add) e
   WHERE t3.fid=t0.fid 
   AND t3.day<'{{ macros.ds_add(ds, 1) }}'
   AND t3.link_to_add is not NULL) as link_to_add,

   (SELECT COUNT(DISTINCT e)
   FROM ds.daily_links t4, 
   LATERAL unnest(link_to_del) e
   WHERE t4.fid=t0.fid 
   AND t4.day<'{{ macros.ds_add(ds, 1) }}'
   AND t4.link_to_del is not NULL) as link_to_del
	
FROM 
(select distinct fid from ds.daily_links) as t0