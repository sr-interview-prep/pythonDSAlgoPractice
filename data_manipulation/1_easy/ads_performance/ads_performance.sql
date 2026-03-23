with t1 as(
select ad_id, sum(case when action in ('Clicked') then 1 else 0 end) as clicked
from ads
group by ad_id
)

, t2 as
(
Select ad_id as ad, sum(case when action in ('Clicked','Viewed') then 1 else 0 end) as total
from ads
group by ad_id
)

Select a.ad_id, coalesce(round((clicked +0.0)/nullif((total +0.0),0)*100,2),0) as ctr
from
(
select *
from t1 join t2
on t1.ad_id = t2.ad) a
order by ctr desc, ad_id