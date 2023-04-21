-- 마케팅 비용 Nora 님이 만든 데이터 테이블을 이용
-- MKT expense 230410
with fix_month as (
select max(a.base_m) fix_mm
from ba_preserved.mkt_cost_m a )
,freecash as (
select cast(wd.first_of_month as varchar) base_m,
       wd.first_of_week,
       count(wd.date) over (partition by wd.year_month) days,
       cast(mf."value" as integer) / count(wd.date) over (partition by wd.year_month) daily_freecash
from temp.fin_mkt_freecash mf
left join ba_preserved.calendar wd on to_char(wd.date,'yyyymm') = substring(mf.yyyymmdd,1,6)
where wd."date" < current_date )
,raw_data as (
select a.base_m,
       wd.first_of_week,
       sum(a.cost) cost,
       0 freecash,
       sum(a.cost) daily_cost,
       0 fix_cost
from ba_preserved.mkt_cost_d a
left join fix_month fm on fm.fix_mm >= a.base_m
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
where fm.fix_mm is null
group by 1,2
union all
select a.base_m,
       wd.first_of_week,
       sum(a.cost) cost,
       0 freecash,
       0 daily_cost,
       sum(a.cost) fix_cost
from ba_preserved.mkt_cost_m a
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
group by 1,2
union all
select fc.base_m,
       fc.first_of_week,
       0 cost,
       sum(fc.daily_freecash) freecash,
       0 daily_cost,
       0 fix_cost
from freecash fc
group by 1,2 )
select a.base_m,
       a.first_of_week,
       sum(-a.cost) cost,
       sum(a.freecash) freecash,
       sum(-a.daily_cost) daily_cost,
       sum(-a.fix_cost) fix_cost
from raw_data a
group by 1,2
order by 1,2