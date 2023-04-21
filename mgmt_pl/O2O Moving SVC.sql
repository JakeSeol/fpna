-- 23/01/18 : O2O Moving Revenue 쿼리 추가
-- 23/02/15 : 이사매출에 /1.1 추가 (부가세반영)
-- 7.O2O Moving Svc. Revenue
select to_char(cast(coalesce(s.basis_date_time) as date),'yyyymm') yyyymm,
       wd.first_of_week,
        count(case when s.sales_status = 2 then s.id end) rev_count,
       -count(case when s.sales_status = 3 then s.id end) cancel_count,
        sum(case when s.sales_status = 2 then  1 when s.sales_status = 3 then -1 else 0 end * coalesce(s.paid_cash_amount,0))/1.1 paid_rev,
        sum(case when s.sales_status = 2 then  1 when s.sales_status = 3 then -1 else 0 end * coalesce(s.free_cash_amount,0))/1.1 free_rev
from  dump_payment_o2o.sales s
left join ba_preserved.calendar wd on wd.date = cast(s.basis_date_time as date)
where s.is_deleted = 0 and s.sales_status in (2,3)
group by 1,2
order by 1,2

