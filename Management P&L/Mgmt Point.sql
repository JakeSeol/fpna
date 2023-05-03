-- 22/12/14 : 1.포인트 코드 변경 (구매대가 1,171 -> 1만으로)
-- 23/03/06 : Point 쿼리 일부 수정 (cast as varchar)
-- 23/04/27 : MSA 포인트 테이블로 변경
-- 23/05/03 : ACCUM / WITHDRAW 금액 컬럼 추가
-- 2. Point Give
-- MSA Point Give
with confrim_order as (
select concat(coalesce(sc.biz,case when coalesce(op.seller_id,opl.seller_id) is not null then '3P'end),'-') biz,
       p.id point_id
from dump_point.points p
left join dump.order_options oo         on cast(oo.id  as varchar) = p.request_id and p.category_type = 'CONFIRM_ORDER'
left join dump.order_productions op     on op.id = oo.order_production_id
left join dump.order_options_l ool      on cast(ool.id as varchar) = p.request_id and p.category_type = 'CONFIRM_ORDER' and ool.base_dt = '2022-12-01'
-- 22년 12월 중순부터 5년 경과 데이터 삭제하기 시작하였으므로 최초를 12월 1일로 설정, 적어도 4년간은 걱정할 필요 없음
left join dump.order_productions_l opl  on opl.id = ool.order_production_id and opl.base_dt = ool.base_dt
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = coalesce(op.seller_id,opl.seller_id)
where p.category_type = 'CONFIRM_ORDER'
  and p.point_type = 'POINT'
group by 1,2 )
,short_point as (
-- 만료일이 1개월 이내인 포인트의 포인트 아이디만 검출
select p.id point_id
from dump_point.points p
where p.process_type in ('ACCUM','WITHDRAW')
  and date_diff('month',cast(p.created_at as date),p.use_end_date) <= 0 )
,first_accum as (
-- MSA 이후 최초 부여 포인트 카테고리를 알수 있는 포인트 추적
select pd.detail_first_accum_id,
       p.category_type,
       co.biz
from dump_point.points p
inner join dump_point.point_details pd   on pd.point_id = p.id -- and pd.version <> 'm'
left join confrim_order co on co.point_id = p.id
where pd.detail_first_accum_id = pd.id
group by 1,2,3 )
select to_char(p.created_at,'yyyymm') tran_mm,
       wd.first_of_week,
       concat(coalesce(co.biz,fa.biz,''),pm.dept,case when pm.dept = 'OTHER' and sp.point_id is not null then '-SHORT' when pm.dept = 'OTHER' then '-NORMAL' else '' end) department,
       sum(coalesce(pd.amount,p.amount)) point,
       round(sum(coalesce(pd.amount,p.amount) / case when pm.dept = 'REWARD' and co.biz = 'FOREIGN' then 1 else 1.1 end),0) as ex_vat_point,
       sum(if(p.process_type = 'ACCUM',coalesce(pd.amount,p.amount),0)) accum_point,
       round(sum(if(p.process_type = 'ACCUM',coalesce(pd.amount,p.amount) / case when pm.dept = 'REWARD' and co.biz = 'FOREIGN' then 1 else 1.1 end),0),0) as accum_ex_vat_point,
       sum(if(p.process_type = 'WITHDRAW',coalesce(pd.amount,p.amount),0)) withdraw_point,
       round(sum(if(p.process_type = 'WITHDRAW',coalesce(pd.amount,p.amount) / case when pm.dept = 'REWARD' and co.biz = 'FOREIGN' then 1 else 1.1 end),0),0) as withdraw_ex_vat_point
from dump_point.points p
left join dump_point.point_details pd on pd.point_id = p.id
left join ba_preserved.calendar wd on wd.date = cast(p.created_at as date)
left join confrim_order co on co.point_id = p.id
left join short_point sp on sp.point_id = p.id
left join first_accum fa on fa.detail_first_accum_id = pd.detail_first_accum_id
left join finance.mileage_point_type_mapping pm on pm.category_type = coalesce(fa.category_type,p.category_type)
where p.process_type in ('ACCUM', 'WITHDRAW')
  and p.service_id = 'OHOUSE'
  and cast(p.created_at as date) >= cast('2019-01-01' as date)
group by 1,2,3
order by 1,2,3
