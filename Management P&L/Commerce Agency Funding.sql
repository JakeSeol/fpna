-- 3. Commerce Agency Funding
select a.yyyymm,
       wd.first_of_week,
       a.logisticstype,
       a.cate_name,
       date_diff('day',cast(substring(a.yyyymm,1,4)||'-'||substring (a.yyyymm,5,2)||'-01' as date), cast(substring(a.yyyymm,1,4)||'-'||substring (a.yyyymm,5,2)||'-01' as date) + interval '1' month - interval '1' day) + 1 date_diff,
       sum(cast(a.funding_value as double)) / ( date_diff('day',cast(substring(a.yyyymm,1,4)||'-'||substring (a.yyyymm,5,2)||'-01' as date), cast(substring(a.yyyymm,1,4)||'-'||substring (a.yyyymm,5,2)||'-01' as date) + interval '1' month - interval '1' day) + 1 ) daily_funding_value
from finance.fin_vendor_funding_md a
left join ba_preserved.calendar wd on to_char(wd.date,'yyyymm') = a.yyyymm
where wd."date" < current_date
group by 1,2,3,4
order by 1,2,3,4
