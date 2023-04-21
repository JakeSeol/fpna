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
;
-- USE Detail
with cancel_amount as (
select p.id,
       p.amount,
       sum(coalesce(cancel.amount,0)) + sum(coalesce(revert.amount,0)) cancel_point,
       sum(coalesce(cancel.amount,0)) total_cancel_point,
       sum(coalesce(revert.amount,0)) revert_cancel_point,
       case when p.amount = -sum(cancel.amount) - sum(coalesce(revert.amount,0)) then 1 else 0 end full_cancel,
       max(greatest(cancel.created_at,revert.created_at)) cancel_dt
from dump_point.points p
inner join dump_point.points cancel on cancel.original_id = p.id        and cancel.process_type = 'USE_CANCEL' and to_char(cancel.created_at,'yyyymm') < '202304'
left join dump_point.points revert  on revert.original_id = cancel.id   and revert.process_type = 'REVERT_USE_CANCEL' and cancel.process_type = 'USE_CANCEL' and to_char(revert.created_at,'yyyymm') < '202304'
where p.process_type in ('USE')
  and p.point_type = 'POINT' and p.service_id = 'OHOUSE'
group by 1,2
)
-- ,raw_data as (
select p.id point_id,
       p.version,
       p.user_id,
       case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end as order_id,
       p.amount use_point,
       to_char(p.created_at,'yyyymm') use_mm,
       case when count(distinct case when coalesce(oo.status,ool.status) in (5,8,9,10) then coalesce(oo.id,ool.id) else null end) = count(distinct coalesce(oo.id,ool.id)) then 1 else 0 end fix,
       case when count(distinct case when coalesce(oo.status,ool.status) in (5,8,9,10) then coalesce(oo.id,ool.id) else null end) = count(distinct coalesce(oo.id,ool.id)) then 1 else 0 end fix_mm,
       sum(coalesce(oo.selling_cost+case when oo.delivery_pay_at = 1 then oo.delivery_fee else 0 end+case when oo.delivery_pay_at in (1,3) and oo.is_backwoods = true then oo.delivery_backwoods else 0 end,ool.selling_cost+case when ool.delivery_pay_at = 1 then ool.delivery_fee else 0 end+case when ool.delivery_pay_at in (1,3) and ool.is_backwoods = true then ool.delivery_backwoods else 0 end )) total_amt,
       sum(case when coalesce(oo.status,ool.status) in (5) and coalesce(sc.biz,'3P') in     ('1P','PREMIUM') then coalesce(oo.selling_cost+case when oo.delivery_pay_at = 1 then oo.delivery_fee else 0 end+case when oo.delivery_pay_at in (1,3) and oo.is_backwoods = true then oo.delivery_backwoods else 0 end,ool.selling_cost+case when ool.delivery_pay_at = 1 then ool.delivery_fee else 0 end+case when ool.delivery_pay_at in (1,3) and ool.is_backwoods = true then ool.delivery_backwoods else 0 end ) else 0 end) confirm_1p,
       sum(case when coalesce(oo.status,ool.status) in (5) and coalesce(sc.biz,'3P') not in ('1P','PREMIUM') then coalesce(oo.selling_cost+case when oo.delivery_pay_at = 1 then oo.delivery_fee else 0 end+case when oo.delivery_pay_at in (1,3) and oo.is_backwoods = true then oo.delivery_backwoods else 0 end,ool.selling_cost+case when ool.delivery_pay_at = 1 then ool.delivery_fee else 0 end+case when ool.delivery_pay_at in (1,3) and ool.is_backwoods = true then ool.delivery_backwoods else 0 end ) else 0 end) confirm_3p,
       sum(case when coalesce(oo.status,ool.status) in (8,9,10)                                              then coalesce(oo.selling_cost+case when oo.delivery_pay_at = 1 then oo.delivery_fee else 0 end+case when oo.delivery_pay_at in (1,3) and oo.is_backwoods = true then oo.delivery_backwoods else 0 end,ool.selling_cost+case when ool.delivery_pay_at = 1 then ool.delivery_fee else 0 end+case when ool.delivery_pay_at in (1,3) and ool.is_backwoods = true then ool.delivery_backwoods else 0 end ) else 0 end) cancel_amt,
       sum(case when coalesce(oo.status,ool.status) not in (5,8,9,10)                                        then coalesce(oo.selling_cost+case when oo.delivery_pay_at = 1 then oo.delivery_fee else 0 end+case when oo.delivery_pay_at in (1,3) and oo.is_backwoods = true then oo.delivery_backwoods else 0 end,ool.selling_cost+case when ool.delivery_pay_at = 1 then ool.delivery_fee else 0 end+case when ool.delivery_pay_at in (1,3) and ool.is_backwoods = true then ool.delivery_backwoods else 0 end ) else 0 end) notfix_amt,
       count(distinct coalesce(oo.id,ool.id)) total_option,
       count(distinct case when coalesce(oo.status,ool.status) in     (5,8,9,10) then coalesce(oo.id,ool.id) else null end) fix_option,
       count(distinct case when coalesce(oo.status,ool.status) in     (5)        then coalesce(oo.id,ool.id) else null end) fix_confirm,
       count(distinct case when coalesce(oo.status,ool.status) not in (5,8,9,10) then coalesce(oo.id,ool.id) else null end) notfix_option
from dump_point.points p
left join dump.order_productions op         on cast(op.order_id as varchar) = case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end
left join dump.order_options oo     on oo.order_production_id = op.id
left join dump.order_option_cancels ooc     on ( ooc.status = 2 or (ooc.status = 3 and ooc.return_fee = 0) ) and ooc.order_option_id = oo.id
left join dump.order_productions_l opl      on opl.base_dt  = '2022-04-30' and op.id is null and cast(opl.order_id as varchar) = case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end
left join dump.order_options_l ool          on ool.base_dt  = '2022-04-30' and ool.order_production_id = opl.id
left join dump.order_option_cancels_l oocl  on oocl.base_dt = '2022-04-30' and ( oocl.status = 2 or (oocl.status = 3 and oocl.return_fee = 0) )  and oocl.order_option_id = ool.id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = coalesce(op.seller_id,opl.seller_id)
where p.point_type = 'POINT' and p.service_id = 'OHOUSE'
  and to_char(p.created_at,'yyyymm') = '201809'
  and p.process_type in ('USE')
--   and case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end = '2078435'
group by 1,2,3,4,5,6
having count(distinct case when coalesce(oo.status,ool.status) in (5,8,9,10) then coalesce(oo.id,ool.id) else null end) <> count(distinct coalesce(oo.id,ool.id))
limit 10000
;

;
)
select a.version,
       a.use_mm,
       a.fix,
       a.fix_mm,
       sum(a.fix_point) fix_point,
       sum(a.use_point) use_point,
       sum(a.confirm_amt) confirm_amt,
       sum(a.confirm_1p) confirm_1p,
       sum(a.confirm_3p) confirm_3p,
       sum(a.cancel_point) cancel_point,
       sum(a.total_cancel_point) total_cancel_point,
       sum(a.revert_cancel_point) revert_cancel_point,
       count(distinct a.order_id) order_count,
       max(a.order_id) sample_order,
       max(a.point_id) sample_point
from raw_data a
group by 1,2,3,4
order by 1,2,3 desc,4
;
-- Point Flow retry
with first_accum as (
select pd.detail_first_accum_id,
       p.category_type,
       p.version,
       p.created_at accum_dt
from dump_point.points p
left join dump_point.point_details pd   on pd.point_id = p.id
where pd.detail_first_accum_id = pd.id
group by 1,2,3,4
)
,step_1 as (
select cast(p.created_at as date) tran_dt,
       cast(coalesce(fa.accum_dt,if(p.process_type='ACCUM',p.created_at)) as date) accum_dt,
       coalesce(pd.use_end_date,p.use_end_date) exp_date,
       cast(date_diff('month',cast(coalesce(fa.accum_dt,if(p.process_type='ACCUM',p.created_at)) as date),coalesce(pd.use_end_date,p.use_end_date)) > 1 as varchar) over_1mnth,
       coalesce(pd.use_end_date,p.use_end_date) < current_date - interval '1' day exp_date_pass,
       p.process_type,
       coalesce(fa.category_type,mig.category_type,p.category_type) category_type,
       if(coalesce(fa.category_type,mig.category_type,p.category_type)='CONFIRM_ORDER',coalesce(sc.biz,'3P')) biz,
       coalesce(fa.version,'migration') version,
       if(fa.detail_first_accum_id is null or p.version = 'm','migration','msa') type,
       p.user_id,
       p.id point_id,
       pd.id detail_id,
       pd.detail_first_accum_id,
       coalesce(pd.amount,p.amount) amount,
       round(cast(p.amount as double) * mh.mileage / sum(mh.mileage) over (partition by p.id),0) dist_amount,
       row_number() over (partition by p.id order by mig.id desc) rank
from dump_point.points p
left join dump_point.point_details pd   on pd.point_id = p.id
left join first_accum fa on fa.detail_first_accum_id = pd.detail_first_accum_id
-- Migration 포인트 부여 카테고리 확인목적(일부만 확인됨)
left join finance.fin_mileage_histories_d mh    on p.version = 'm' and p.process_type in ('USE','WITHDRAW','EXPIRE') and mh.user_id = cast(p.user_id as varchar) and mh.mileage_id = cast(p.payload as varchar)
left join dump_point.points mig                on p.version = 'm' and mig.version =  'm' and mh.reference_mileage_id = cast(mig.payload as varchar) and mh.user_id = cast(mig.user_id as varchar)
left join dump.order_options oo                 on cast(oo.id as varchar) = coalesce(mig.request_id,p.request_id) and coalesce(mig.category_type,p.category_type) = 'CONFIRM_ORDER'
-- 구매확정 포인트의 비지니스를 구분하기 위해 만듦
left join dump.order_productions op     on op.id = oo.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = op.seller_id
where p.service_id = 'OHOUSE'
--   and p.user_id = 17295843
)
,step_2 as (
select a.type,
       to_char(a.tran_dt,'yyyymm') tran_mm,
       coalesce(to_char(a.accum_dt,'yyyymm'),'unknown') acumm_mm,
       coalesce(to_char(a.exp_date,'yyyymm'),'unknown') exp_mm,
       coalesce(cast(a.over_1mnth as varchar),'unknown') over_1mnth,
       coalesce(cast(a.exp_date_pass as varchar),'unknown') exp_date_pass,
       a.biz,
       a.category_type,
       a.process_type,
       pm.dept,
       case when pm.category_code in ('1') then 'REWARD'
            when pm.category_code in ('2','3','5','15','16','134','135','136','140','18','137','138','139','155','181') then 'ECOMM'
            when pm.category_code in ('183') then 'ECOMM-1P'
            when pm.category_code in ('182') then 'ECOMM-PREMIUM'
            when pm.category_code in ('186') then 'RENTAL'
            when pm.category_code in ('17','21','22','24','26','30','142','143','144','145','146','147','148','149','161','162','163','164','184','187') then 'CON'
            when pm.category_code in ('10','172') then 'CS'
            when pm.category_code in ('156') then 'HR-EMPLOYEE'
            when pm.category_code in ('160') then 'HR-HIRING'
            when pm.category_code in ('11','12','14','23','27','141','185') then 'MKT'
            when pm.category_code in ('19','20','25','28','150','151','152','153','154','188') then 'O2O'
            when pm.category_code in ('189') then 'O2O-MOV'
            when pm.category_code in ('190') then 'O2O-CON'
            when pm.category_code in ('193') then 'O2O-REPAIR'
            when pm.category_code in ('194') then 'O2O-CLEAN'
            when pm.category_code in ('191') then 'XR'
            when pm.category_code in ('192') then 'UX'
            when pm.category_code in ('301','302','303','304','305') then 'O2O_CASH'
            when pm.category_code in ('4','29') then 'CANCEL'
            else pm.category_code end dept_mgmtpl,
       a.version,
       a.user_id,
       a.point_id,
       a.detail_id,
       a.detail_first_accum_id,
       coalesce(case when a.rank = 1 then a.amount - sum(a.dist_amount) over (partition by a.point_id) + a.dist_amount else a.dist_amount end,a.amount) amount,
       a.rank
from step_1 a
left join finance.mileage_point_type_mapping pm on pm.category_type = a.category_type
)
select a.type,
       a.tran_mm,
       a.acumm_mm,
       a.exp_mm,
       a.over_1mnth,
       a.exp_date_pass,
       a.dept,
       a.dept_mgmtpl,
       a.biz,
       a.category_type,
       a.process_type,
       a.version,
       sum(a.amount) amount,
       max(a.user_id) sample_user,
       max(a.point_id) sample_pointid,
       max(a.detail_id) sample_detail_id,
       max(a.detail_first_accum_id) sample_first
from step_2 a
group by 1,2,3,4,5,6,7,8,9,10,11,12
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
select o.id,
       oo.id,
       oo.delivery_start_date,
       oo.delivery_complete_date,
       oo.purchase_confirm_date,
       ooc.updated_at,
       count(oo.id) option_id
from dump.order_options oo
left join dump.order_option_cancels ooc on ooc.order_option_id = oo.id and ooc.status in (2,3)
left join dump.order_productions op on op.id = oo.order_production_id
left join dump.orders o on o.id = op.order_id
group by 1,2,3,4,5,6
-- where o.id = 1834471
having count(oo.id) > 1
limit 10000
;
