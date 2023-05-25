-- 22/10/18 : 퍼포먼스 마케팅 5번 항목에 추가
-- 22/10/24 : 1P 매출액, GMV에 배송조립비 포함하도록 변경
-- 22/11/16 : 1P와 SKU의 매핑을 인식하는 서브쿼리 변경
-- 22/11/21 : 배송조립비 매출 인식 시작, 결제기준 11/01 부터
-- 22/12/14 : fin_coupon_conditions_md과 fin_coupon_conditions_md_old 를 union 후 join으로 변경
-- 23/01/30 : Kuro님 1P 쿼리 반영
-- 23/02/13 : Instant discount funding 추가
-- 23/03/06 : finance.fin_transport_fee_calculate_options_monthly 테이블의 중복 옵션 제거
-- 23/03/06 : round 삽입 및 coupon_cost 관련 총액 - 나머지 = other 로 계산방식변경, Mgmt PL로 명칭 변경
-- 23/03/20 : finance.fin_sku_monthly_orderprice의 방어로직 추가
-- 23/03/27 : Coupon 테이블 변경 : Cart/Product
-- 23/04/10 : 카드프로모션 테이블 변경, 포인트 타입 중 O2O 설치수리, O2O 입주청소 추가
-- 1. Commerce
with item_sku_mapped as (
select cast(a.item_id as integer) as item_id
     , a.sku_code
     , a.quantity as sku_count
from (
select *
     , row_number() over(partition by item_id, sku_code order by created_at desc) as sku_rank
     , rank() over(partition by item_id order by created_at desc) as item_rank
     , created_at as start_date
     , lag(created_at) over(partition by item_id, sku_code order by created_at desc) as end_date
from dump_logistics_nosnos.product_mapping_histories
where item_id != sku_code ) a
where a.sku_rank = 1
  and a.item_rank = 1 )
,max_inv as (
select max(a.yyyymm) max_yyyymm
from finance.fin_sku_monthly_orderprice a
)
,inventory as (
select mi.max_yyyymm is not null latest_inv, a.yyyymm, b.item_id, count(distinct a.skucode) as sku_count, sum(b.sku_count) as qty, sum(cast(if(a.orderprice_ex_vat='-','0',a.orderprice_ex_vat) as bigint)*b.sku_count) as orderprice_ex_vat_per_item
from finance.fin_sku_monthly_orderprice a --22년부터
join item_sku_mapped b on a.skucode = b.sku_code
left join max_inv mi on mi.max_yyyymm = a.yyyymm
group by 1,2,3
having sum(cast(if(a.orderprice_ex_vat='-','0',a.orderprice_ex_vat) as bigint)*b.sku_count)  > 0 )
,raw_data as (
-- 2019년 데이터
select to_char(cast(a.order_at as date),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.order_at as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       a.product_id,
       sum(a.selling_cost) gmv,
       sum(round(case when sc.biz = 'FOREIGN' then a.selling_cost - a.supply_cost else ( a.selling_cost - a.supply_cost ) / 1.1 end,0) ) revenue,
       0 dab_revenue,
       sum(case when a.delivery_pay_at = 1 then a.delivery_fee else 0 end) + sum(a.assemble_fee) + sum(case when a.delivery_pay_at in (1,3) and a.is_backwoods = true then a.delivery_backwoods else 0 end) dab,
       sum(a.quantity) qty,
       0 cogs, 0 direct_delivery_cost,
       0 coupon_cost_ecom, 0 coupon_cost_con, 0 coupon_cost_mkt, 0 coupon_cost_other, 0 coupon_cost, 0 deduct_ecom, 0 deduct_con, 0 deduct_mkt, 0 deduct_other, 0 deduct, 0 deposit_ecom, 0 deposit_con, 0 deposit_mkt, 0 deposit_other, 0 deposit, 0 card_discount, 0 ohouse_card_discount, 0 partner_card_discount,
       0 instant_discount_funding
from quicksight.payment a
left join ba_preserved.calendar wd on wd.date = cast(a.order_at as date)
left join dump.order_productions b on b.id = a.order_product_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = b.seller_id
left join dump.productions p on p.id = b.production_id
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
where a.order_at >= '2019-01-01'
  and a.order_at <  '2019-09-01'
  and a."date" = '2019-09-01'
group by 1,2,3,4,5,6
union all
select to_char(cast(a.cancel_at as date),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.cancel_at as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       a.product_id,
       -sum(a.selling_cost) gmv,
       -sum(round(case when sc.biz = 'FOREIGN' then a.selling_cost - a.supply_cost else ( a.selling_cost - a.supply_cost ) / 1.1 end,0) ) revenue,
       0 dab_revenue,
       -sum(if(a.delivery_pay_at = 1, a.delivery_fee, 0)) + sum(a.assemble_fee) + sum(if(a.delivery_pay_at in (1,3) and a.is_backwoods = true, a.delivery_backwoods, 0)) dab,
       -sum(a.quantity) qty,
       0 cogs, 0 direct_delivery_cost,
       0 coupon_cost_ecom, 0 coupon_cost_con, 0 coupon_cost_mkt, 0 coupon_cost_other, 0 coupon_cost, 0 deduct_ecom, 0 deduct_con, 0 deduct_mkt, 0 deduct_other, 0 deduct, 0 deposit_ecom, 0 deposit_con, 0 deposit_mkt, 0 deposit_other, 0 deposit, 0 card_discount, 0 ohouse_card_discount, 0 partner_card_discount,
       0 instant_discount_funding
from quicksight.refund a
left join dump.order_productions b on b.id = a.order_product_id
left join ba_preserved.calendar wd on wd.date = cast(a.cancel_at as date)
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = b.seller_id
left join dump.productions p on p.id = b.production_id
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
where a.cancel_at >= '2019-01-01'
  and a.cancel_at <  '2019-09-01'
  and a."date" = '2019-09-01'
group by 1,2,3,4,5,6
union all
select to_char(cast(a.date as date),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.date as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       a.product_id,
       sum(case when sc.biz in ('1P','PREMIUM') then a.selling_cost + a.delivery + a.assemble + a.backwoods
                else a.selling_cost end) gmv,
       sum(round(case when sc.biz in ('1P','PREMIUM') then ( a.selling_cost + a.delivery + a.assemble + a.backwoods ) / 1.1
                when sc.biz = 'BOOK' then a.selling_cost
                when sc.biz = 'FOREIGN' then a.selling_cost - a.supply_cost
                else ( a.selling_cost - a.supply_cost ) / 1.1 end,0)) -- product revenue
       + sum(round(case when o.payment_at >= cast('2022-11-01' as date) and sc.biz is null then 0.03
                when o.payment_at >= cast('2022-11-01' as date) and sc.biz = 'FOREIGN' then 0.033
                else 0 end * ( a.delivery + a.assemble + a.backwoods ),0) ) -- dab_revenue from 11/01
         revenue,
       sum(round(case when o.payment_at >= cast('2022-11-01' as date) and sc.biz is null then 0.03
                when o.payment_at >= cast('2022-11-01' as date) and sc.biz = 'FOREIGN' then 0.033
                else 0 end * ( a.delivery + a.assemble + a.backwoods ),0) ) dab_revenue,
       sum(a.delivery + a.assemble + a.backwoods) dab,
       sum(a.quantity) qty,
       -sum(round(case when sc.biz = '1P' and b.type = 'PB'       then coalesce(coalesce(i1.orderprice_ex_vat_per_item,i2.orderprice_ex_vat_per_item)*a.quantity,(a.selling_cost/1.1)*0.69) -- PB 상품
                       when sc.biz = '1P' and b.type = 'RB'       then coalesce(coalesce(i1.orderprice_ex_vat_per_item,i2.orderprice_ex_vat_per_item)*a.quantity,(a.selling_cost/1.1)*0.74) -- RB 상품
                       when sc.biz = '1P' and b.type = '사입'      then coalesce(coalesce(i1.orderprice_ex_vat_per_item,i2.orderprice_ex_vat_per_item)*a.quantity,(a.selling_cost/1.1)*0.79) -- 사입 상품
                       when sc.biz = '1P' and b.type is null      then coalesce(coalesce(i1.orderprice_ex_vat_per_item,i2.orderprice_ex_vat_per_item)*a.quantity,(a.selling_cost/1.1)*0.83) -- 프리미엄 후판매
                       when sc.biz = 'PREMIUM' and b.type is null then coalesce(coalesce(i1.orderprice_ex_vat_per_item,i2.orderprice_ex_vat_per_item)*a.quantity,(a.selling_cost/1.1)*0.83) -- 프리미엄 선판매
                       else 0 end,0)) cogs, --★
       -sum(case when sc.biz in ('1P','PREMIUM') then a.quantity * coalesce(d.calculated_transport_fee,0) else 0 end) as direct_delivery_cost,
       0 coupon_cost_ecom, 0 coupon_cost_con, 0 coupon_cost_mkt, 0 coupon_cost_other, 0 coupon_cost, 0 deduct_ecom, 0 deduct_con, 0 deduct_mkt, 0 deduct_other, 0 deduct, 0 deposit_ecom, 0 deposit_con, 0 deposit_mkt, 0 deposit_other, 0 deposit, 0 card_discount, 0 ohouse_card_discount, 0 partner_card_discount,
       0 instant_discount_funding
from ba_preserved.commerce_snapshot_payment_result a
left join dump.order_options oo on oo.id = a.order_option_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = a.seller_id
left join dump.productions p on p.id = a.product_id
left join dump.orders o on o.id = a.order_id
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
left join ba_preserved.calendar wd on wd.date = cast(a.date as date)
left join max_inv mi on mi.max_yyyymm < to_char(cast(a.date as date),'yyyymm')
left join inventory i1 on ( to_char(date_add('month',-1,cast(a.date as date)),'yyyymm') = i1.yyyymm or ( to_char(cast(a.date as date),'yyyymm') < '202201' and i1.yyyymm = '202201' ) ) --★
                                                                                      and a.reference_option_id = i1.item_id --월별 회계 단가
left join inventory i2 on mi.max_yyyymm is not null and i2.latest_inv = true and a.reference_option_id = i2.item_id --월별 회계 단가
left join finance.fin_transport_fee_calculate_options_monthly as d on a.reference_option_id = cast(d.option_id as bigint) and to_char(cast(a.date as date),'yyyymm') = d.yyyymm
left join ba.pbc_products b on a.product_id = b.production_id
left join dump.production_properties dp on dp.production_id = a.product_id and dp.property_number = 0 --★
where a.date >= '2019-09-01'
and cast(a."date" as date) < current_date
group by 1,2,3,4,5,6
union all
select to_char(cast(base_dt as date),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.base_dt as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       cast(a.production_id as integer) product_id,
       0 gmv,  0 revenue, 0 dab_revenue, 0 dab,  0 qty, 0 cogs, 0 direct_delivery_cost,
       -sum(case when coalesce(cpm.department,coalesce(cm.department,4)) = 0 then a.coupon_cost else 0 end) coupon_cost_ecom,
       -sum(case when coalesce(cpm.department,coalesce(cm.department,4)) = 1 then a.coupon_cost else 0 end) coupon_cost_con,
       -sum(case when coalesce(cpm.department,coalesce(cm.department,4)) = 3 then a.coupon_cost else 0 end) coupon_cost_mkt,
       -sum(case when coalesce(cpm.department,coalesce(cm.department,4)) not in (0,1,3) then a.coupon_cost else 0 end) coupon_cost_other,
       -sum(a.coupon_cost) coupon_cost,
       0 deduct_ecom, 0 deduct_con, 0 deduct_mkt, 0 deduct_other, 0 deduct, 0 deposit_ecom, 0 deposit_con, 0 deposit_mkt, 0 deposit_other, 0 deposit, 0 card_discount, 0 ohouse_card_discount, 0 partner_card_discount,
       0 instant_discount_funding
from (
     select * from finance.fin_coupon_cart_order_options
     union all
     select * from finance.fin_coupon_product_order_options
     ) a
left join dump.coupon_molds cm on cm.id = cast(a.coupon_mold_id as integer)
left join (
     select coupon_mold_id
          , case when pcm.service_id = 'COMMERCE' then 0
                 when pcm.service_id = 'CONTENTS' then 1
                 when pcm.service_id = 'MARKETING' then 3
                 else 4 end as department
     from dump_coupon.coupon_molds pcm
     where coupon_type = 'PRODUCT'
     ) cpm on cpm.coupon_mold_id = cast(a.coupon_mold_id as integer)
left join dump.productions p on p.id = cast(a.production_id as integer)
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
left join temp.fin_non_3p_seller_current sc on sc.seller_id = a.seller_id
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
where a.base_dt >= '2019-01-01'
and cast(a.base_dt as date) < current_date
group by 1,2,3,4,5,6
union all
select to_char(cast(base_dt as date),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.base_dt as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       cast(a.production_id as integer) product_id,
       0 gmv,  0 revenue, 0 dab_revenue, 0 dab,  0 qty, 0 cogs, 0 direct_delivery_cost,
       0 coupon_cost_ecom, 0 coupon_cost_con, 0 coupon_cost_mkt, 0 coupon_cost_other, 0 coupon_cost,
       sum(case when coalesce(a.department,4) = 0 then a.coupon_cost_seller_distributed_deduction else 0 end) deduct_ecom,
       sum(case when coalesce(a.department,4) = 1 then a.coupon_cost_seller_distributed_deduction else 0 end) deduct_con,
       sum(case when coalesce(a.department,4) = 3 then a.coupon_cost_seller_distributed_deduction else 0 end) deduct_mkt,
       sum(case when coalesce(a.department,4) not in (0,1,3) then a.coupon_cost_seller_distributed_deduction else 0 end) deduct_other,
       sum(a.coupon_cost_seller_distributed_deduction) deduct,
       sum(case when coalesce(a.department,4) = 0 and ccm.post_settlement = '0' then a.coupon_cost_seller_distributed_deposit else 0 end) deposit_ecom,
       sum(case when coalesce(a.department,4) = 1 and ccm.post_settlement = '0' then a.coupon_cost_seller_distributed_deposit else 0 end) deposit_con,
       sum(case when coalesce(a.department,4) = 3 and ccm.post_settlement = '0' then a.coupon_cost_seller_distributed_deposit else 0 end) deposit_mkt,
       sum(case when coalesce(a.department,4) not in (0,1,3) and ccm.post_settlement = '0' then a.coupon_cost_seller_distributed_deposit else 0 end) deposit_other,
       sum(case when ccm.post_settlement = '0' then a.coupon_cost_seller_distributed_deposit else 0 end) deposit,
       0 card_discount, 0 ohouse_card_discount, 0 partner_card_discount,
       0 instant_discount_funding
from finance.fin_coupon_order_option_distributions a
left join ( select * from finance.fin_coupon_conditions_md union select * from finance.fin_coupon_conditions_md_old ) ccm on a.coupon_mold_id = ccm.id
left join temp.fin_non_3p_seller_current sc on sc.seller_id = a.seller_id
left join dump.productions p on p.id = cast(a.production_id as integer)
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
where a.base_dt >= '2019-01-01'
and cast(a.base_dt as date) < current_date
group by 1,2,3,4,5,6
union all
select to_char(to_date(a.base_dt,'yyyy-mm-dd'),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.base_dt as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       cast(a.production_id as integer) product_id,
       0 gmv,  0 revenue, 0 dab_revenue, 0 dab,  0 qty, 0 cogs, 0 direct_delivery_cost,
       0 coupon_cost_ecom, 0 coupon_cost_con, 0 coupon_cost_mkt, 0 coupon_cost_other, 0 coupon_cost,
       0 deduct_ecom, 0 deduct_con, 0 deduct_mkt, 0 deduct_other, 0 deduct, 0 deposit_ecom, 0 deposit_con, 0 deposit_mkt, 0 deposit_other, 0 deposit,
       sum(a.promotion_cost) card_discount,
      -sum(a.promotion_cost_ohouse_dist) ohouse_card_discount,
      -sum(a.promotion_cost_partner_dist) partner_card_discount,
       0 instant_discount_funding
from finance.fin_card_promotion_order_history_distributions a
left join dump.order_productions b on b.id = cast(a.order_production_id as integer)
left join dump.productions p on p.id = b.production_id
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = b.seller_id
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
where a.base_dt >= '2019-01-01'
and cast(a.base_dt as date) < current_date
group by 1,2,3,4,5,6
union all
select to_char(to_date(a.base_dt,'yyyy-mm-dd'),'yyyymm') yyyymm,
       wd.first_of_week,
       cast(a.base_dt as date) base_date,
       coalesce(sc.biz,'3P') biz,
       cate.name_depth1,
       cast(a.production_id as integer) product_id,
       0 gmv,  0 revenue, 0 dab_revenue, 0 dab,  0 qty, 0 cogs, 0 direct_delivery_cost,
       0 coupon_cost_ecom, 0 coupon_cost_con, 0 coupon_cost_mkt, 0 coupon_cost_other, 0 coupon_cost,
       0 deduct_ecom, 0 deduct_con, 0 deduct_mkt, 0 deduct_other, 0 deduct, 0 deposit_ecom, 0 deposit_con, 0 deposit_mkt, 0 deposit_other, 0 deposit,
       0 card_discount, 0 ohouse_card_discount, 0 partner_card_discount,
       sum(a.seller_distribution) as instant_discount_funding
from finance.fin_discount_order_option_distributions_d a
left join dump.order_productions b on b.id = cast(a.order_production_id as integer)
left join dump.productions p on p.id = b.production_id
left join finance.fin_admin_categories_md cate on cate.admin_category_id = p.admin_category_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = b.seller_id
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
where a.base_dt >= '2019-01-01'
and cast(a.base_dt as date) < current_date
group by 1,2,3,4,5,6
)
select a.yyyymm,
       a.first_of_week,
       a.biz || case when a.biz = '1P' and dp.id is not null then '-PREMIUM' else '' end biz,
       coalesce(a.name_depth1,'N/A') || coalesce('-'||b.type,'') category,
       sum(a.gmv) gmv,
       sum(a.revenue) revenue,
--        sum(a.dab_revenue) dab_revenue,
       sum(a.dab) dab,
       sum(a.qty) qty,
       sum(a.cogs) cogs,
       sum(a.direct_delivery_cost) direct_delivery_cost,
       sum(round(a.coupon_cost / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) coupon_cost,
       sum(round(a.coupon_cost_ecom / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) coupon_cost_ecom,
       sum(round(a.coupon_cost_mkt / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) coupon_cost_mkt,
       sum(round(a.coupon_cost_con / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) coupon_cost_con,
       sum(round(a.coupon_cost / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )
      -sum(round(a.coupon_cost_ecom / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )
      -sum(round(a.coupon_cost_mkt / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )
      -sum(round(a.coupon_cost_con / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) coupon_cost_other,
       sum(round(a.deduct / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) deduct,
       sum(round(a.deduct_ecom / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) deduct_ecom,
       sum(round(a.deduct_mkt / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) deduct_mkt,
       sum(round(a.deduct_con / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) ) deduct_con,
       sum(round(a.deduct / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )
      -sum(round(a.deduct_ecom / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )
      -sum(round(a.deduct_mkt / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )
      -sum(round(a.deduct_con / case when a.biz = 'FOREIGN' then 1 else 1.1 end,0) )  deduct_other,
       sum(a.deposit) deposit,
       sum(a.deposit_ecom) deposit_ecom,
       sum(a.deposit_mkt) deposit_mkt,
       sum(a.deposit_con) deposit_con,
       sum(a.deposit) - sum(a.deposit_ecom) - sum(a.deposit_mkt) - sum(a.deposit_con) deposit_other,
       sum(a.card_discount) card_discount,
       sum(a.ohouse_card_discount) ohouse_card_discount,
       sum(a.partner_card_discount) partner_card_discount,
       sum(a.instant_discount_funding) instant_discount_funding,
       max(a.product_id) sample_product_id
from raw_data a
left join ba.pbc_products b on a.product_id = b.production_id
left join dump.production_properties dp on dp.production_id = a.product_id and dp.property_number = 0
where a.base_date < current_date
group by 1,2,3,4
order by 1,2,3,4