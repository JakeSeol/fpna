select p.id point_id,
       p.version,
       p.user_id,
       case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end as order_id,
       p.amount use_point,
       count(distinct case when coalesce(oo.status,ool.status) in (5,8,9,10) then coalesce(oo.id,ool.id) end) = count(distinct coalesce(oo.id,ool.id)) order_fix,
       count(distinct coalesce(oo.id,ool.id)) option_total,
       count(distinct case when coalesce(oo.status,ool.status) in (5,8,9,10) then coalesce(oo.id,ool.id) end) option_fix,
       count(distinct case when coalesce(oo.status,ool.status) in (5       ) then coalesce(oo.id,ool.id) end) option_confirm,
       count(distinct case when coalesce(oo.status,ool.status) in (8,10    ) then coalesce(oo.id,ool.id) end) option_refund,
       count(distinct case when coalesce(oo.status,ool.status) in (9       ) then coalesce(oo.id,ool.id) end) option_expire
from dump_point.points p
left join dump.order_productions op         on cast(op.order_id as varchar) = case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end
left join dump.order_options oo     on oo.order_production_id = op.id
left join dump.order_productions_l opl      on opl.base_dt  = '2022-04-30' and op.id is null and cast(opl.order_id as varchar) = case when p.version = 'v1' and p.payload is not null and split_part(p.payload,'-',2) is null then p.payload else p.request_id end
left join dump.order_options_l ool          on ool.base_dt  = '2022-04-30' and ool.order_production_id = opl.id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = coalesce(op.seller_id,opl.seller_id)
where p.point_type = 'POINT' and p.service_id = 'OHOUSE'
  and to_char(p.created_at,'yyyy') < '2020'
  and p.process_type in ('USE')
group by 1,2,3,4,5
having count(distinct case when coalesce(oo.status,ool.status) in (5,8,9,10) then coalesce(oo.id,ool.id) end) <> count(distinct coalesce(oo.id,ool.id))
limit 10000
;
-- 결제 후 옵션의 상태값이 5,8,9,10 중의 하나가 아닌 주문아이디
-- 결제 성공 여부 추가
-- Issue #1 : Option Cleansing
with option_cancel as (
select ooc.id,ooc.order_option_id,ooc.status,ooc.before_confirm,ooc.reason,ooc.reason_text,ooc.bank_name,ooc.bank_account,ooc.created_at,ooc.updated_at,ooc.return_fee,ooc.bank_owner,ooc.return_mileages,ooc.image_url,ooc.recovery_address,ooc.recovery_address_detail,ooc.recovery_postcode,ooc.return_coupons,ooc.revision_history,ooc.before_status,ooc.seller_id,
       row_number() over (partition by ooc.order_option_id order by ooc.updated_at desc) rank
from dump.order_option_cancels ooc )
,option_exchange as (
select oe.id,oe.order_option_id,oe.status,oe.reason,oe.reason_text,oe.created_at,oe.updated_at,oe.image_url,oe.recovery_address,oe.recovery_address_detail,oe.recovery_postcode,oe.seller_id,
       row_number() over (partition by oe.order_option_id order by oe.updated_at desc) rank
from dump.order_options oo
left join dump.order_option_exchanges oe on oe.order_option_id = oo.id )
,status_desc   as ( select 0 status, '0:입금대기' description union all select 1 status, '1:결제완료' description union all select 2 status, '2:배송준비' description union all select 3 status, '3:배송중' description union all select 4 status, '4:배송완료' description union all select 5 status, '5:구매확정' description union all select 6 status, '6:환불진행' description union all select 7 status, '7:교환진행' description union all select 8 status, '8:계좌만료' description union all select 9 status, '9:환불완료' description union all select 10 status, '10:결제이탈' description )
,cancel_status as ( select 0 status, '0:신청' description union all select 1 status, '1:거절' description union all select 2 status, '2:완료' description union all select 3 status, '3:입금대기' description )
,exch_status   as ( select 0 status, '0:신청' description union all select 1 status, '1:거절' description union all select 2 status, '2:승인' description )
select concat('https://admin.dailyhou.se/orders/',cast(o.id as varchar)) order_link,
       to_char(o.payment_at,'yyyymm') pay_mm,
       o.imp_uid,
       o.merchant_uid,
       o.id order_id,
       oo.id option_id,
       op.seller_id,
       su.company,
       date_diff('day',o.payment_at,current_date) payment_since,
       p.need_customer_number is_foreign,
       if(p.need_customer_number,date_diff('day',o.payment_at,current_date)) forein_since,
       ac.id is not null is_rental,
       oo.selling_cost = 0 is_zero,
       op.delivery_method = 1 is_seller_delivery,
       coalesce(sc.biz in ('PREMIUM','1P'),false) is_premium,
       if(sc.biz in ('PREMIUM','1P'),date_diff('day',oo.confirm_order_date,current_date)) premium_since,
       coalesce(sd.description,'NULL') option_status,
       cs.description cancel_status,
       if(ooc.status= 0 and oo.status = 6,date_diff('day',ooc.updated_at,current_date)) cancel_since,
       es.description exch_status,
       if(oe.status = 0 and oo.status = 7,date_diff('day',oe.updated_at,current_date)) exch_since,
       case when ooc.status = 2 then '결제취소여부 확인요,결제취소시9로변경요'
            when op.seller_id = 13893950 then '1P 배송 확인 필요'
            when oo.status is null and oo.confirm_order_date        is null then '결제취소여부 확인요,결제취소시9로변경요'
            when oo.status is null and oo.purchase_confirm_date     is not null then '5로 상태변경요'
            when oo.status is null and oo.delivery_complete_date    is not null then '4로 상태변경 및 구매확정처리요'
            when oo.status is null then '판매자확인요'

            when oo.status = 1 and ac.id is not null and oo.selling_cost = 0 then '렌탈 주문:클렌징 필요'
            when oo.status = 1 and if(sc.biz in ('PREMIUM','1P'),date_diff('day',o.payment_at,current_date)) <  365 then '프리미엄 1년 미경과'
            when oo.status = 1 and if(sc.biz in ('PREMIUM','1P'),date_diff('day',o.payment_at,current_date)) >= 365 then '프리미엄 1년 경과:프리미엄팀 확인요'
            when oo.status = 1 and op.delivery_method = 1 then '직접배송상품:배송여부 확인요'
            when oo.status = 1 and oo.purchase_confirm_date is not null then '5로 상태변경요'
            when oo.status = 1 then '판매자확인요'

            when oo.status = 4 and oo.delivery_start_date is null and oo.delivery_complete_date is null then '배송시작/완료일 없음'
            when oo.status = 4 and oo.delivery_complete_date is null then '배송완료일 없음'
            else 'to be defined' end option_issue,
       cast(o.payment_at as date) payment_dt,
       cast(oo.confirm_order_date as date) confirm_order_dt,
       cast(oo.delivery_start_date as date) delivery_start_dt,
       cast(oo.delivery_complete_date as date) delivery_complete_dt,
       cast(oo.purchase_confirm_date as date) purchase_confirm_dt,
       cast(oo.calculate_date as date) calculate_dt,
       oo.is_calculated,
       oo.selling_cost
from dump.orders o
inner join dump.order_productions op on op.order_id = o.id
inner join dump.order_options oo on oo.order_production_id = op.id
left join status_desc sd on sd.status = oo.status
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = coalesce(op.seller_id)
left join dump.productions p    on p.id = op.production_id
left join dump.users u on p.user_id = u.id and u.userable_type = 'SalesUser'
left join dump.sales_users su on u.userable_id = su.id
left join dump.admin_categories ac on ac.id = p.admin_category_id and trim(split_part(ac.full_display_name,'>',1)) = '렌탈'
left join option_cancel ooc  on ooc.rank = 1 and ooc.order_option_id = oo.id
left join cancel_status cs on cs.status = ooc.status
left join option_exchange oe on oe.rank  = 1 and oe.order_option_id  = oo.id
left join exch_status es on es.status = oe.status
where ( (o.is_payment_success = true and o.payment_method != 'without_bankbook') or (o.is_payment_success = true and o.payment_method = 'without_bankbook' and o.is_success_vbank = true) ) -- 결제 성공 조건 추가
  and to_char(o.payment_at,'yyyymm') < '202204'
  and (oo.status not in (5,8,9,10) or oo.status is null)
--   and oo.status is null
order by o.id,oo.id
;
select to_char(o.payment_at,'yyyymm') pay_mm,
       o.id,
       oo.id option_id,
       count(distinct oo.id) option_total,
       count(distinct case when   oo.status in (5,8,9,10)                           then oo.id end) option_fix,
       count(distinct case when   oo.status not in (5,8,9,10) or oo.status is null  then oo.id end) option_remain,
       count(distinct case when ( oo.status not in (5,8,9,10) or oo.status is null ) and  p.need_customer_number = true then oo.id end) remain_need_customer,
       count(distinct case when ( oo.status not in (5,8,9,10) or oo.status is null ) and  ac.id is not null             then oo.id end) remain_rental,
       count(distinct case when ( oo.status not in (5,8,9,10) or oo.status is null ) and  sc.biz = 'PREMIUM'            then oo.id end) remain_premium,
       count(distinct case when ( oo.status not in (5,8,9,10) or oo.status is null ) and  oo.purchase_confirm_date is not null and oo.calculate_date is not null then oo.id end) remain_confirm_settle,

       count(distinct case when oo.status is null then oo.id end)                                                                                                                status_null,               -- 모르겠고 상태값이 null, issue #2 에서 해결
       count(distinct case when oo.status = 1 and oo.purchase_confirm_date is not null then oo.id end)                                                                           confirmed_status_changed,  -- 구매확정/정산까지 되었으나, 1.결제완료 상태, 상태값만 5로 변경
       count(distinct case when oo.status = 1 and oo.purchase_confirm_date is null and date_diff('day',o.payment_at,current_date) > 30 and oo.selling_cost = 0 then oo.id end)   zero_product_no_confirm,   -- 0원상품 구매확정 안됨
       count(distinct case when oo.status = 2 and date_diff('day',oo.confirm_order_date,current_date)  > 30 then oo.id end)                                                      preparation,               -- 배송준비 30일 이상
       count(distinct case when oo.status = 3 and oo.purchase_confirm_date is not null and oo.delivery_complete_date is null then oo.id end)                                     confirmed_no_complete,     --
       count(distinct case when oo.status = 3 and date_diff('day',oo.delivery_start_date,current_date) > 30 and oo.purchase_confirm_date is null and oo.delivery_start_date is not null  then oo.id end) delivery_inprogress,            -- 배송시작후 30일초과
       count(distinct case when oo.status = 3 and date_diff('day',oo.confirm_order_date,current_date)  > 30 and oo.purchase_confirm_date is null and oo.delivery_start_date is null      then oo.id end) delivery_inprogress_date_null,  -- 배송중상태, 배송시작일자 없음
       count(distinct case when oo.status = 4 and oo.purchase_confirm_date is null and date_diff('day',oo.delivery_complete_date,current_date) < 9 and oo.delivery_complete_date < current_date then oo.id end)  delivery_complete_in_7day, -- 배송완료후 8일 이내이며 구매확정 안됨. 잠시 뒤 구매확정 될것임. 해야할일 없음
       count(distinct case when oo.status = 4 and oo.delivery_complete_date is null then oo.id end)                                     delivery_complete_date_null,    -- 배송완료 상태이나 배송완료일자 없음
       count(distinct case when oo.status = 4 and oo.delivery_complete_date > current_date then oo.id end)                              complete_date_error,            -- 배송완료일이 미래여서 구매확정 되지 않음. 배송완료일 수정 후, 구매확정처리

       count(distinct case when oo.status = 6 and ooc.status = 2 then oo.id end)                                                        refunded_status_error,          -- 환불승인,환불되었으나 Option의 상태값만 6.환불진행중. 상태값만 9로 변경
       count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is not null then oo.id end)               refund_inprogress_cancel_table_null_confirmed, -- 상태 환불진행중,
       count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is null     then oo.id end)               refund_inprogress_cancel_table_null_not_confirmed,
       count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is not null and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)  refund_inprogress_confirmed,     -- 환불신청 후 30일 경과. 구매확정됨
       count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is null     and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)  refund_inprogress_not_confirmed, -- 환불신청 후 30일 경과. 구매확정안됨
       count(distinct case when oo.status = 7 and oe.status = 0 and date_diff('day',oe.updated_at,current_date) > 30 then oo.id end)    exchange_inprogress,            -- 교환신청 후 30일 경과. 교환거절해야함

       count(distinct case when oo.status is null then oo.id end)
     + count(distinct case when oo.status = 1 and oo.purchase_confirm_date is not null then oo.id end)
     + count(distinct case when oo.status = 1 and oo.purchase_confirm_date is null and date_diff('day',o.payment_at,current_date) > 30 and oo.selling_cost = 0 then oo.id end)
     + count(distinct case when oo.status = 2 and date_diff('day',oo.confirm_order_date,current_date)  > 30 then oo.id end)
     + count(distinct case when oo.status = 3 and oo.purchase_confirm_date is not null and oo.delivery_complete_date is null then oo.id end)
     + count(distinct case when oo.status = 3 and date_diff('day',oo.delivery_start_date,current_date) > 30 and oo.purchase_confirm_date is null and oo.delivery_start_date is not null  then oo.id end)
     + count(distinct case when oo.status = 3 and date_diff('day',oo.confirm_order_date,current_date)  > 30 and oo.purchase_confirm_date is null and oo.delivery_start_date is null      then oo.id end)
     + count(distinct case when oo.status = 4 and oo.purchase_confirm_date is null and date_diff('day',oo.delivery_complete_date,current_date) < 9 and oo.delivery_complete_date < current_date then oo.id end)
     + count(distinct case when oo.status = 4 and oo.delivery_complete_date is null then oo.id end)
     + count(distinct case when oo.status = 4 and oo.delivery_complete_date > current_date then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.status = 2 then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is not null then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is null     then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is not null and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is null     and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)
     + count(distinct case when oo.status = 7 and oe.status = 0 and date_diff('day',oe.updated_at,current_date) > 30 then oo.id end)
     + count(distinct case when oo.status in (5,8,9,10) then oo.id end) = count(distinct oo.id) reason_found,

       count(distinct case when oo.status is null then oo.id end)
     + count(distinct case when oo.status = 1 and oo.purchase_confirm_date is not null then oo.id end)
     + count(distinct case when oo.status = 1 and oo.purchase_confirm_date is null and date_diff('day',o.payment_at,current_date) > 30 and oo.selling_cost = 0 then oo.id end)
     + count(distinct case when oo.status = 2 and date_diff('day',oo.confirm_order_date,current_date)  > 30 then oo.id end)
     + count(distinct case when oo.status = 3 and oo.purchase_confirm_date is not null and oo.delivery_complete_date is null then oo.id end)
     + count(distinct case when oo.status = 3 and date_diff('day',oo.delivery_start_date,current_date) > 30 and oo.purchase_confirm_date is null and oo.delivery_start_date is not null  then oo.id end)
     + count(distinct case when oo.status = 3 and date_diff('day',oo.confirm_order_date,current_date)  > 30 and oo.purchase_confirm_date is null and oo.delivery_start_date is null      then oo.id end)
     + count(distinct case when oo.status = 4 and oo.purchase_confirm_date is null and date_diff('day',oo.delivery_complete_date,current_date) < 9 and oo.delivery_complete_date < current_date then oo.id end)
     + count(distinct case when oo.status = 4 and oo.delivery_complete_date is null then oo.id end)
     + count(distinct case when oo.status = 4 and oo.delivery_complete_date > current_date then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.status = 2 then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is not null then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is null     then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is not null and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)
     + count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is null     and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)
     + count(distinct case when oo.status = 7 and oe.status = 0 and date_diff('day',oe.updated_at,current_date) > 30 then oo.id end)
       reason_count,

       sum(case when oo.status not in (5,8,9,10) or oo.status is null then oo.selling_cost+if(oo.delivery_pay_at = 1,oo.delivery_fee,0)+if(oo.delivery_pay_at in (1,3) and oo.is_backwoods = true, oo.delivery_backwoods, 0) else 0 end) notfix_total_amt,
       sum(case when oo.status not in (5,8,9,10) or oo.status is null then oo.supply_cost else 0 end) notfix_supply_amt
from dump.orders o
left join dump.order_productions op         on op.order_id = o.id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = coalesce(op.seller_id)
left join dump.productions p    on p.id = op.production_id
left join dump.admin_categories ac on ac.id = p.admin_category_id and trim(split_part(ac.full_display_name,'>',1)) = '렌탈'
left join dump.order_options oo     on oo.order_production_id = op.id
left join option_cancel ooc on ooc.rank = 1 and ooc.order_option_id = oo.id
left join option_exchange oe on oe.rank =1  and oe.order_option_id = oo.id
where ( (o.is_payment_success = true and o.payment_method != 'without_bankbook') or (o.is_payment_success = true and o.payment_method = 'without_bankbook' and o.is_success_vbank = true) ) -- 결제 성공 조건 추가
  and to_char(o.payment_at,'yyyymm') < '202301'
--   and o.id = 101165503
group by 1,2
having count(distinct case when oo.status in (5,8,9,10) then oo.id end) <> count(distinct oo.id)
-- having count(distinct case when oo.status in (4) and oo.delivery_complete_date is null then oo.id end)
--      + count(distinct case when oo.status is null then oo.id end)
--      + count(distinct case when oo.status = 2 and date_diff('day',oo.confirm_order_date,current_date) > 30 then oo.id end)
--      + count(distinct case when oo.status = 3 and date_diff('day',oo.delivery_start_date,current_date) > 30 then oo.id end)
--      + count(distinct case when oo.status = 3 and date_diff('day',oo.confirm_order_date,current_date)  > 30 and oo.delivery_start_date is null then oo.id end)
--      + count(distinct case when oo.status = 7 and date_diff('day',oe.updated_at,current_date) > 30 then oo.id end)
--      + count(distinct case when oo.status = 1 and oo.purchase_confirm_date is not null then oo.id end)
--      + count(distinct case when oo.status = 6 and ooc.status = 2 then oo.id end)
--      + count(distinct case when oo.status = 4 and oo.delivery_complete_date > current_date then oo.id end)
--      + count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is not null then oo.id end)
--      + count(distinct case when oo.status = 6 and ooc.id is null and oo.purchase_confirm_date is null     then oo.id end)
--      + count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is not null and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)
--      + count(distinct case when oo.status = 6 and ooc.status = 0 and oo.purchase_confirm_date is null     and date_diff('day',ooc.updated_at,current_date) > 30 then oo.id end)
--      + count(distinct case when oo.status = 1 and oo.purchase_confirm_date is null and date_diff('day',o.payment_at,current_date) > 30 and oo.selling_cost = 0 then oo.id end)
--      + count(distinct case when oo.status = 4 and oo.purchase_confirm_date is null and date_diff('day',oo.delivery_complete_date,current_date) < 9 and oo.delivery_complete_date < current_date then oo.id end)
--      + count(distinct case when oo.status in (5,8,9,10) then oo.id end) <> count(distinct oo.id)
order by 1,2
;
-- Issue #2. option status is null
select o.id,
       oo.id option_id,
       o.created_at order_created_at,
       o.payment_at,
       oo.confirm_order_date,
       oo.delivery_start_date,
       oo.delivery_complete_date,
       oo.purchase_confirm_date,
       ooc.status cancel_status,
       ooc.updated_at cancel_updated_at,
       oe.status exchange_status,
       oe.updated_at exchange_updated_at,
       -- 데이터 상태를 보고 예측되는 status 결정
       case when ooc.status = 2 then 9
            when oo.purchase_confirm_date   is not null then 5
            when oo.delivery_complete_date  is not null then 4
            when oo.delivery_start_date     is not null then 3
            when oo.confirm_order_date      is not null then 2
            when o.payment_at               is not null then 1
            end should_be_option_status
-- oo.status,oo.id,oo.purchase_confirm_date
from dump.order_options oo
left join dump.order_option_cancels ooc on ooc.order_option_id = oo.id
left join dump.order_option_exchanges oe on oe.order_option_id = oo.id
inner join dump.order_productions op on op.id = oo.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = coalesce(op.seller_id)
inner join dump.orders o on op.order_id = o.id
where ( (o.is_payment_success = true and o.payment_method != 'without_bankbook') or (o.is_payment_success = true and o.payment_method = 'without_bankbook' and o.is_success_vbank = true) ) -- 결제 성공 조건 추가
  and oo.status is null
limit 3000
;
-- 결제취소 되었거나,
-- 배송완료 되었는데, 못잡고 있거나,
-- 진짜 주문확인안하고 대기중이거나,

-- 렌탈 이슈

