-- 분석 결과가 이 금액과 동일하면 됨
-- Should be : total
select p.version,
       p.process_type,
       sum(p.amount) amt
from dump_point.points p
where p.point_type = 'POINT' and p.service_id = 'OHOUSE'
--   and to_char(p.created_at,'yyyymm') < '202304'
group by 1,2
order by 1,2
;
-- 탈퇴회원의 만료처리가 제대로 되지 않고 있으나, 향후 개선될 것임
-- 잔액의 스냅샷인 user_points 테이블과 거래원장인 points의 합산이 맞지 않는것은 모두 탈퇴회원의 만료처리 때문임.
-- 포인트 잔액차이(탈퇴회원)
with point_bal as (
select p.user_id,
       sum(p.amount) data_sum,
       0 user_bal,
       count(p.id) tran_count
from dump_point.points p
where p.service_id = 'OHOUSE'
group by 1
union all
select up.user_id,
       0 data_sum,
       sum(up.balance) user_bal,
       0 tran_count
from dump_point.user_points up
where up.service_id = 'OHOUSE'
group by 1,2 )
select pb.user_id,
       u.id is null withdrawal,
       sum(pb.data_sum) data_sum,
       sum(pb.user_bal) user_bal,
       sum(pb.data_sum) - sum(pb.user_bal) diff,
       sum(pb.tran_count) tran_count
from point_bal pb
left join dump.users u on u.id = pb.user_id
group by 1,2
having sum(pb.data_sum) <> sum(pb.user_bal)
;
-- POINT Report
with first_accum as (
-- first accum 이 있는 포인트지급의 지급카테고리/구매비지니스 구분
select pd.detail_first_accum_id,
       p.process_type,
       pm.acct_type acct_cate,
       if(p.category_type='CONFIRM_ORDER',if(sc.biz in ('1P','PREMIUM'),'1P','3P')) accum_acct_biz,
       p.category_type,
       p.version,
       p.created_at
from dump_point.points p
inner join dump_point.point_details pd   on pd.point_id = p.id -- and pd.version <> 'm'
left join dump.order_options oo          on cast(oo.id as varchar) = p.request_id and p.category_type = 'CONFIRM_ORDER'
-- 구매확정 포인트의 비지니스를 구분하기 위해 만듦
left join dump.order_productions op on op.id = oo.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = op.seller_id
left join finance.mileage_point_type_mapping pm on pm.category_type = p.category_type
where pd.detail_first_accum_id = pd.id
group by 1,2,3,4,5,6,7
)
,step_1 as (
select if(fa.detail_first_accum_id is not null and fa.version <> 'm','MSA','LEGACY') accum_ver,
       cast(p.created_at as date) tran_dt,
       cast(coalesce(if(p.process_type='ACCUM',p.created_at),fa.created_at,mig.created_at) as date) accum_dt,
       coalesce(pd.use_end_date,p.use_end_date,mig.use_end_date) exp_date,
       cast(date_diff('month',cast(coalesce(if(p.process_type='ACCUM',p.created_at),fa.created_at,mig.created_at) as date),coalesce(pd.use_end_date,p.use_end_date,mig.use_end_date)) > 1 as varchar) over_1mnth,
       case when coalesce(pd.use_end_date,p.use_end_date,mig.use_end_date) is null then 'unknown' else cast(coalesce(pd.use_end_date,p.use_end_date,mig.use_end_date) < current_date - interval '1' day as varchar) end exp_date_pass, -- Pass 는 mig을 사용하지 않음. mig
       p.version,
       p.id point_id,
       pd.id detail_id,
       pd.detail_first_accum_id,
       mig.id legacy_accum_id,
       p.user_id,
       p.process_type,
       p.category_type tran_cate,
       case when coalesce(if(p.process_type='ACCUM',p.category_type),fa.category_type,mig.category_type) in ('CONFIRM_ORDER','DECORATIVE_SUPPORT_FUND','EXPIRE','USE_FOR_ORDER','WITHDRAW_BY_ADMIN','WITHDRAW_BY_CANCEL_CONFIRM') then coalesce(if(p.process_type='ACCUM',p.category_type),fa.category_type,mig.category_type)
            when coalesce(if(p.process_type='ACCUM',p.category_type),fa.category_type,mig.category_type) in ('ORDER_CANCEL','ORDER_REFUND') then 'ORDER_CANCEL/REFUND'
            when coalesce(if(p.process_type='ACCUM',p.category_type),fa.category_type,mig.category_type) is not null then 'EVENT'
            else 'UNKNOWN' end accum_acct_cate,
       if(coalesce(if(p.process_type='ACCUM',p.category_type),fa.category_type,mig.category_type) = 'CONFIRM_ORDER',if(sc.biz in ('1P','PREMIUM'),'1P','3P'),fa.accum_acct_biz) accum_acct_biz,
       mp.dept accum_dept,
       coalesce(if(p.process_type='ACCUM',p.category_type),fa.category_type,mig.category_type,'UNKNOWN') accum_cate,
       coalesce(pd.amount,p.amount) amount,
       p.amount point_amount,
       pd.amount detail_amount,
       round(cast(coalesce(pd.amount,p.amount) as double) * coalesce(mh.mileage,1) / sum(coalesce(mh.mileage,1)) over (partition by coalesce(pd.id,p.id)),0) dist_amount,
       row_number() over (partition by coalesce(pd.id,p.id) order by coalesce(mig.id,fa.detail_first_accum_id)) rank
from dump_point.points p
left join dump_point.point_details pd           on pd.point_id = p.id --and pd.version <> 'm'
-- points 테이블과 detail 테이블을 조인하여 최초 지급내역을 알수 있는데이터를 추적
left join first_accum fa                        on fa.detail_first_accum_id = pd.detail_first_accum_id
-- with 에서 적용한 first_accum_id 로 추적가능한 대부분의 포인트 데이터를 추적
left join finance.fin_mileage_histories_d mh    on fa.version is null and mh.user_id = cast(p.user_id as varchar) and mh.mileage_id = p.payload
-- first_accum_id 로 추적이 안되는것은 finance 테이블로 추적
left join dump_point.points mig                 on mh.reference_mileage_id = cast(mig.payload as varchar) and mh.user_id = cast(mig.user_id as varchar)
-- finance 테이블과 MSA points 테이블을 payload(구 mileage id)로 연결
left join dump.order_options oo                 on cast(oo.id as varchar) = coalesce(mig.request_id,p.request_id) and coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type) = 'CONFIRM_ORDER'
left join dump.order_productions op     on op.id = oo.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = op.seller_id
-- first_accum_id 가 추적되지 않는 구매확정 포인트의 비지니스를 구분하기 위해 만듦
left join finance.mileage_point_type_mapping mp on mp.category_type = coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type)
where p.point_type = 'POINT' and p.service_id = 'OHOUSE'
)
,step_2 as (
-- 일자를 월로 변경, 값을 찾을수 없는 경우 unknown 으로 분류
-- finance 테이블의 금액을 사용하는 경우 배분후 단수 조정
select a.accum_ver,
       a.version tran_ver,
       to_char(a.tran_dt,'yyyymm') tran_mm,
       coalesce(to_char(a.accum_dt,'yyyymm'),'unknown') acumm_mm,
       coalesce(to_char(a.exp_date,'yyyymm'),'unknown') exp_mm,
       coalesce(cast(a.over_1mnth as varchar),'unknown') over_1mnth,
       coalesce(cast(a.exp_date_pass as varchar),'unknown') exp_date_pass,
       a.accum_acct_cate,
       concat(a.accum_acct_cate,if(a.accum_acct_biz is null, '', concat('-',a.accum_acct_biz))) accum_acct_cate_biz,
       a.accum_dept,
       concat(a.accum_dept,if(a.accum_acct_biz is null, '', concat('-',a.accum_acct_biz))) accum_dept_biz,
       a.accum_cate,
       a.tran_cate,
       a.process_type,
       a.user_id,
       a.point_id,
       a.detail_id,
       a.detail_first_accum_id,
       a.legacy_accum_id,
       a.amount total_amount,
       coalesce(case when a.process_type = 'ACCUM' then a.amount
                     when a.rank = 1 then a.amount - sum(coalesce(a.dist_amount,0)) over (partition by a.detail_id,a.point_id) + coalesce(a.dist_amount,0)
                     else coalesce(a.dist_amount,0) end,coalesce(a.amount,0)) amount,
       a.rank
from step_1 a
)
-- 최종 Summarize
select a.accum_ver,
       a.tran_ver,
       a.tran_mm,
       a.accum_mm,
       a.exp_mm,
       a.over_1mnth,
       a.exp_date_pass,
       a.process_type,
       a.accum_acct_cate,
       a.accum_acct_cate_biz,
       a.accum_dept,
       a.accum_dept_biz,
       a.accum_cate,
       a.tran_cate,
       sum(a.amount) amount,
       count(distinct a.point_id) pointid_cnt,
       max(a.user_id) sample_user,
       max(a.point_id) sample_pointid,
       max(a.detail_id) sample_detail_id,
       max(coalesce(a.detail_first_accum_id,a.legacy_accum_id)) sample_first
from step_2 a
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14