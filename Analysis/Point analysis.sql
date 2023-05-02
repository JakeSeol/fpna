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
inner join dump_point.point_details pd   on pd.point_id = p.id and pd.version <> 'm'
left join dump.order_options oo          on cast(oo.id as varchar) = p.request_id and p.category_type = 'CONFIRM_ORDER'
-- 구매확정 포인트의 비지니스를 구분하기 위해 만듦
left join dump.order_productions op on op.id = oo.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = op.seller_id
left join finance.mileage_point_type_mapping pm on pm.category_type = p.category_type
where pd.detail_first_accum_id = pd.id
group by 1,2,3,4,5,6,7
)
,step_1 as (
select if(fa.detail_first_accum_id is not null,'MSA','LEGACY') accum_ver,
       cast(p.created_at as date) tran_dt,
       cast(coalesce(fa.created_at,mig.created_at,if(p.process_type='ACCUM',p.created_at)) as date) accum_dt,
       coalesce(pd.use_end_date,p.use_end_date,mig.use_end_date) exp_date,
       cast(date_diff('month',cast(coalesce(fa.created_at,mig.created_at,if(p.process_type='ACCUM',p.created_at)) as date),coalesce(pd.use_end_date,p.use_end_date,mig.use_end_date)) > 1 as varchar) over_1mnth,
       coalesce(pd.use_end_date,p.use_end_date) < current_date - interval '1' day exp_date_pass, -- Pass 는 mig을 사용하지 않음. mig
       p.version,
       p.id point_id,
       pd.id detail_id,
       pd.detail_first_accum_id,
       mig.id legacy_accum_id,
       p.user_id,
       p.process_type,
       p.category_type tran_cate,
       fa.category_type fa_cate,
       mig.category_type legacy_cate,
       coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type,'UNKNOWN') accum_cate,
       case when coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type) in ('CONFIRM_ORDER','DECORATIVE_SUPPORT_FUND','EXPIRE','USE_FOR_ORDER','WITHDRAW_BY_ADMIN','WITHDRAW_BY_CANCEL_CONFIRM') then coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type)
            when coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type) in ('ORDER_CANCEL','ORDER_REFUND') then 'ORDER_CANCEL/REFUND'
            when coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type) is not null then 'EVENT'
            else 'UNKNOWN' end accum_acct_cate,
       if(coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type) = 'CONFIRM_ORDER',if(sc.biz in ('1P','PREMIUM'),'1P','3P'),fa.accum_acct_biz) accum_acct_biz,
       coalesce(pd.amount,p.amount) amount,
       p.amount point_amount,
       pd.amount detail_amount,
       round(cast(coalesce(pd.amount,p.amount) as double) * coalesce(mh.mileage,1) / sum(coalesce(mh.mileage,1)) over (partition by coalesce(pd.id,p.id)),0) dist_amount,
       row_number() over (partition by coalesce(pd.id,p.id) order by coalesce(mig.id,fa.detail_first_accum_id)) rank
from dump_point.points p
left join dump_point.point_details pd           on pd.point_id = p.id and pd.version <> 'm'
left join first_accum fa                        on fa.detail_first_accum_id = pd.detail_first_accum_id

left join finance.fin_mileage_histories_d mh    on fa.version is null and mh.user_id = cast(p.user_id as varchar) and mh.mileage_id = cast(p.payload as varchar)
left join dump_point.points mig                on mh.reference_mileage_id = cast(mig.payload as varchar) and mh.user_id = cast(mig.user_id as varchar)
left join dump.order_options oo                 on cast(oo.id as varchar) = coalesce(mig.request_id,p.request_id) and coalesce(if(p.process_type='ACCUM',p.category_type),mig.category_type,fa.category_type) = 'CONFIRM_ORDER'
-- 구매확정 포인트의 비지니스를 구분하기 위해 만듦
left join dump.order_productions op     on op.id = oo.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = op.seller_id
where p.point_type = 'POINT' and p.service_id = 'OHOUSE'
--   and to_char(p.created_at,'yyyymm') < '202304'
)
,step_2 as (
select a.accum_ver,
       a.version tran_ver,
       to_char(a.tran_dt,'yyyymm') tran_mm,
       coalesce(to_char(a.accum_dt,'yyyymm'),'unknown') acumm_mm,
       coalesce(to_char(a.exp_date,'yyyymm'),'unknown') exp_mm,
       coalesce(cast(a.over_1mnth as varchar),'unknown') over_1mnth,
       coalesce(cast(a.exp_date_pass as varchar),'unknown') exp_date_pass,
       a.accum_acct_cate,
       a.accum_acct_biz,
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
select a.accum_ver,
       a.tran_ver,
       a.tran_mm,
       a.acumm_mm,
       a.exp_mm,
       a.over_1mnth,
       a.exp_date_pass,
       a.process_type,
       a.accum_acct_cate,
       a.accum_acct_biz,
       a.accum_cate,
       a.tran_cate,
       sum(a.amount) amount,
       count(distinct a.point_id) pointid_cnt,
       max(a.user_id) sample_user,
       max(a.point_id) sample_pointid,
       max(a.detail_id) sample_detail_id,
       max(coalesce(a.detail_first_accum_id,a.legacy_accum_id)) sample_first
from step_2 a
group by 1,2,3,4,5,6,7,8,9,10,11,12