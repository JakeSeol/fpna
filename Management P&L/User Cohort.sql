-- 8.User Cohort data
with cashflow as ( -- 5월 1만원 결제, 6월 1만원 환불 > 5월에 +1만 & 6월에 -1만 각각 기록
select user_id, month, sum(paid) + sum(cancel) selling_cost
from ba_preserved.mkt_arppu_summary
where user_id > 0
group by 1,2
)
, total_payment as ( -- 5월 1만원 결제, 6월 1만원 환불 >  5월에 1만-1만 = 0
select user_id, month, sum(paid) + sum(cancel2) selling_cost
from ba_preserved.mkt_arppu_summary
where user_id > 0
group by 1,2
)
select    to_char(date(FirstOrder_m),'yyyymm') yyyymm
        , to_char(date(order_m),'yyyymm') order_mm
        , date_diff('month',date(FirstOrder_m),date(order_m)) month_diff
		, count(distinct if(selling_cost > 0,user_id,null)) order_users
		, sum(selling_cost) selling_cost
from (
-- 5월 1만원 결제, 6월 1만원 환불, 8월 2만원 결제 >  5월에 +1만 & 6월에 -1만 기록은 "5월" 코호트
select c.user_id, c.month as order_m, c.selling_cost, fo.FirstOrder_m
from cashflow c
join total_payment t on c.user_id = t.user_id
					and c.month = t.month
					and t.selling_cost = 0
join (
select user_id, min(month) FirstOrder_m
from cashflow
where selling_cost > 0
and user_id != 16386628 -- 구매 이력 2012년으로 잘못 기재, 가입일 2021-12-21 19:35:54.000, 그다음 구매 22년 1월
group by 1
union all
select 16386628, '2022-01-01' -- 구매 이력 2012년으로 잘못 기재, 가입일 2021-12-21 19:35:54.000, 그다음 구매 22년 1월
) fo on c.user_id = fo.user_id
union all
-- 5월 1만원 결제, 6월 1만원 환불, 8월 2만원 결제 >  8월 2만원은 "8월" 코호트
select c.user_id, c.month as order_m, c.selling_cost, fo.FirstOrder_m
from cashflow c
join total_payment t on c.user_id = t.user_id and c.month = t.month and t.selling_cost != 0
join (
select user_id, min(month) FirstOrder_m
from total_payment
where selling_cost > 0
and user_id != 16386628 -- 구매 이력 2012년으로 잘못 기재, 가입일 2021-12-21 19:35:54.000, 그다음 구매 22년 1월
group by 1
union all
select 16386628, '2022-01-01' -- 구매 이력 2012년으로 잘못 기재, 가입일 2021-12-21 19:35:54.000, 그다음 구매 22년 1월
) fo on c.user_id = fo.user_id
)
--where user_id = 17948576 -- 주석 처리 되어 있는 케이스
where FirstOrder_m <= order_m -- dump 에 selling_cost <=0 으로 수기 변경으로 추정되는 값 존재 (원칙적으로는 환불해도 selling_cost > 0)
-- and  to_char(date(FirstOrder_m),'yyyymm') >= '201901'
group by 1,2,3
order by 1,2,3
