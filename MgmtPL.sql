-- 22/10/18 : 퍼포먼스 마케팅 5번 항목에 추가
-- 22/10/24 : 1P 매출액, GMV에 배송조립비 포함하도록 변경
--            퍼포먼스 마케팅 위클리 실제사용액 반영
-- 22/11/16 : 1P와 SKU의 매핑을 인식하는 서브쿼리 변경
-- 22/11/21 : 배송조립비 매출 인식 시작, 결제기준 11/01 부터
-- 22/12/14 : 1.포인트 코드 변경 (구매대가 1,171 -> 1만으로)
--            2.O2O 시공 GMV/계약건수 데이터 추가
--            3.fin_coupon_conditions_md과 fin_coupon_conditions_md_old 를 union 후 join으로 변경
--            4.퍼포먼스 마케팅 코드 추가(채널 추가)
-- 23/01/16 : O2O Remodeling GMV 쿼리 수정(21년 6월부터 집계)
-- 23/01/18 : O2O Moving Revenue 쿼리 추가
-- 23/01/30 : Kuro님 1P 쿼리 반영
-- 23/02/08 : 첫구매 User Cohort 추가
-- 23/02/13 : Instant discount funding 추가
-- 23/02/15 : 이사매출에 /1.1 추가 (부가세반영)
-- 23/03/06 : Point 쿼리 일부 수정 (cast as varchar), finance.fin_transport_fee_calculate_options_monthly 테이블의 중복 옵션 제거
--            raw data validation 단계 생성, round 삽입 및 coupon_cost 관련 총액 - 나머지 = other 로 계산방식변경, Mgmt PL로 명칭 변경
-- 23/03/20 : finance.monthly_sku_orderprice의 방어로직 추가
-- 23/03/27 : Coupon 테이블 변경 : Cart/Product
-- 23/04/10 : 카드프로모션 테이블 변경, 포인트 타입 중 O2O 설치수리, O2O 입주청소 추가

-- 값이 없어야 함
-- 0. Validation
select 'temp.fin_non_3p_seller_current' table_name, sc.seller_id join_key, count(sc.company_name) row_count
from temp.fin_non_3p_seller_current sc group by 1,2 having count(sc.company_name) > 1
union all
select 'finance.monthly_sku_orderprice' table_name, concat(a.yyyymm,' / ',a.skucode) join_key, count(a.load_dt) row_count
from finance.monthly_sku_orderprice a group by 1,2 having count(a.load_dt) > 1
union all
select 'finance.fin_admin_categories_md' table_name, cast(cate.admin_category_id as varchar) join_key, count(cate.name_depth1)
from finance.fin_admin_categories_md cate group by 1,2 having count(cate.name_depth1) > 1
union all
select 'finance.fin_transport_fee_calculate_options_monthly' table_name, concat(d.yyyymm,' / ',d.option_id) join_key, count(d.basic_transport_fee) row_count
from finance.fin_transport_fee_calculate_options_monthly as d group by 1,2 having count(d.basic_transport_fee) > 1
union all
select 'ba.pbc_products' table_name, cast(b.production_id as varchar) join_key, count(b.type) row_count
from ba.pbc_products b group by 1,2 having count(b.type) > 1
union all
select 'dump.production_properties' table_name, cast(dp.production_id as varchar) join_key, count(dp.id) row_count
from dump.production_properties dp where dp.property_number = 0 group by 1,2 having count(dp.id) > 1
;
-- 1. GMV / Coupon & Settle
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
from finance.monthly_sku_orderprice a
)
,inventory as (
select mi.max_yyyymm is not null latest_inv, a.yyyymm, b.item_id, count(distinct a.skucode) as sku_count, sum(b.sku_count) as qty, sum(cast(if(a.orderprice_ex_vat='-','0',a.orderprice_ex_vat) as bigint)*b.sku_count) as orderprice_ex_vat_per_item
from finance.monthly_sku_orderprice a --22년부터
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
;
-- 2. Point Give
with rewward_biz_data as (
select coalesce(sc.biz,'3P')||case when sc.biz = '1P' and dp.id is not null then '-PREMIUM' else '' end biz,
       a.mileage_id,
       a.reference_id
from finance.fin_mileage_histories_d a
left join dump.order_options c on c.id = cast(a.reference_id as integer)
left join dump.order_productions b on b.id = c.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = b.seller_id
left join dump.production_properties dp on dp.production_id = b.production_id and dp.property_number = 0
where a.mileage_type = '1'
group by 1,2,3
)
,exp_mnth as (
select a.mileage_id,
       to_char(cast(a.base_dt as date),'yyyymm') give_mm,
       case when a.use_end_at is null then 3
            when a.use_end_at > cast(a.base_dt as date) then 0
            else coalesce(date_diff('month',cast(a.base_dt as date),cast(a.use_end_at as date)),3) end exp_mnth,
       case when a.use_end_at < current_date then 'END'
            when a.use_end_at is null then 'NO END'
            when a.use_end_at >= current_date then 'REMAIN' else 'ERROR' end grp
from finance.fin_mileage_histories_d a
where a.mileage_category = 'GIVE'
group by 1,2,3,4
)
,raw_data as (
select to_char(cast(a.base_dt as date),'yyyymm') yyyymm
     , a.base_dt
     , wd.first_of_week
     , coalesce(x.biz,coalesce(sc.biz,'3P')) biz
     , em.exp_mnth
     , case when a.mileage_type in ('1') then coalesce(x.biz,coalesce(sc.biz,'3P'))||'-REWARD'
            when a.mileage_type in ('2','3','5','15','16','134','135','136','140','18','137','138','139','155','181') then 'ECOMM'
            when a.mileage_type in ('183') then 'ECOMM-1P'
            when a.mileage_type in ('182') then 'ECOMM-PREMIUM'
            when a.mileage_type in ('186') then 'RENTAL'
            when a.mileage_type in ('17','21','22','24','26','30','142','143','144','145','146','147','148','149','161','162','163','164','184','187') then 'CON'
            when a.mileage_type in ('10','172') then 'CS'
            when a.mileage_type in ('156') then 'HR-EMPLOYEE'
            when a.mileage_type in ('160') then 'HR-HIRING'
            when a.mileage_type in ('11','12','14','23','27','141','185') then 'MKT'
            when a.mileage_type in ('19','20','25','28','150','151','152','153','154','188') then 'O2O'
            when a.mileage_type in ('189') then 'O2O-MOV'
            when a.mileage_type in ('190') then 'O2O-CON'
            when a.mileage_type in ('193') then 'O2O-REP'
            when a.mileage_type in ('194') then 'O2O-CLE'
            when a.mileage_type in ('191') then 'XR'
            when a.mileage_type in ('192') then 'UX'
            when a.mileage_type in ('4','29') then 'CANCEL'
            when em.exp_mnth = 0 then 'OTHER-SHORT' else 'OTHER-NORMAL' end department
     , -sum(a.mileage) point
from finance.fin_mileage_histories_d a -- 전체
left join rewward_biz_data x on a.reference_mileage_id = x.mileage_id
left join dump.order_options c on cast(c.id as varchar) = a.reference_id and a.mileage_type = '1'
left join dump.order_productions b on b.id = c.order_production_id
left join temp.fin_non_3p_seller_current sc on cast(sc.seller_id as integer) = b.seller_id
left join ba_preserved.calendar wd on wd.date = cast(a.base_dt as date)
left join exp_mnth em on em.mileage_id = case when a.mileage_category = 'GIVE' then a.mileage_id else a.reference_mileage_id end
where a.mileage_type not in ('7','8','9','170','171')
and a.base_dt >= '2019-01-01'
and cast(a.base_dt as date) < current_date
group by 1,2,3,4,5,6
)
select yyyymm tran_yyyymm
     , first_of_week
     , department
     ,sum(point) point
     ,sum(point) / case when department = 'FOREIGN-REWARD' then 1 else 1.1 end as ex_vat_point
from raw_data
group by 1,2,3
order by 1,2,3
;
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
;
-- 4. Product AD Revenue
MSCK REPAIR TABLE advertise.`ad-ledger`
;
-- 4. Product AD Revenue
with raw_data as (
select ledgertype -- 클릭차감 / 광고비 충전의 구분하는 type 값
    , salesuserid -- 판매자 id
    , from_unixtime(clicktimestamp / 1000, 'Asia/Seoul') click_date --클릭시점
    , from_unixtime(ledgertimestamp / 1000, 'Asia/Seoul') ledger_date --데이터 기록된 시점
    , cast(budget.paid as integer) as budget_paid --차감 후 유상 광고비
    , cast(budget.free as integer) as budget_free --차감 후 무상 광고비
    , cast(prevbudget.paid as integer) as prevbudget_paid --차감 전 유상 광고비
    , cast(prevbudget.free as integer) as prevbudget_free --차감 전 무상 광고비
    , cast(cost.cpc as integer) as cost_cpc -- 실제 적용된 cpc
    , cast(cost.vat as integer) as cost_vat -- 실제 적용된 vat
    , cast(cost.ispaid as varchar) as cost_ispaid --광고비의 유상 / 무상 구분 (true면 유상, false면 무상)
    , cast(request.member1.campaignid as varchar) as campaignid -- 설정된 캠페인 id
    , cast(request.member1.cpc as integer) as request_cpc -- 몰로코 to 버킷으로 전달된 cpc 값. 이걸 다 합하면 말씀하신 광고 총 매출이 됩니다.
    , concat("year", "month", "day") as "date"
from advertise."ad-ledger"
where ledgertype = 'CLICK'
)
select to_char(wd."date",'yyyymm') yyyymm,
       wd.first_of_week,
       sum(cost_cpc) as gross_cpc,
       sum(if(cost_ispaid = 'true',cost_cpc, 0)) as paid_cpc,
       sum(if(cost_ispaid = 'false',cost_cpc, 0)) as free_cpc
from raw_data a
left join ba_preserved.calendar wd on to_char(wd.date,'yyyymmdd') = a."date"
where cost_ispaid is not null
and to_char(wd."date",'yyyy-mm-dd') >= '2022-04-01'
group by 1,2
order by 1,2
;
-- 6. O2O Remodeling GMV
with raw_data as (
select  'remodelingstep' as table_nm,
        date(cast(concat(substring(contract.inspecteddate,1,10),' ',substring(contract.inspecteddate,12,8)) as timestamp) + interval '9'hour) as dt_contract, -- 검수일
        date(cast(concat(substring(createdAt,1,10),' ',substring(createdAt,12,8)) as timestamp) + interval '9'hour) as dt_match, -- 매칭일
        date(cast(concat(substring(contract.modifieddownpaymentupdateddate,1,10),' ',substring(contract.modifieddownpaymentupdateddate,12,8)) as timestamp) + interval '9'hour) as modified_date,
        r._id.oid as consultation_id,
        cast(legacyconsultationid as varchar) as legacy_consultation_id,
        coalesce(contract.downpayment,0) as downpayment,
        coalesce(contract.modifieddownpayment,0) as modifieddownpayment,
        user._id as user_id,
        user.name as user_name,
        user.phonenumber as phone_number,
        if(legacyconsultationid in (68853,40620),'리스팅',
        if(legacyconsultationid in (43175,50115,56388),'간편',
        if(inflowchannel in (0,4,5,6,7), '간편',
        if(inflowchannel in (1,2), '리스팅',
        if(inflowchannel = 3,'시공스토어',cast(inflowchannel as varchar)))))) as type,
        -- 업체정보
       r.expertuser._id as expert_user_id,
       if(r.expertise.scale = 0 or r.expertise.scale = 9999999 and e5.expert_user_id is not null,'종합',
       if(r.expertise.scale = 1 or r.expertise.scale = 9999999 and e5.expert_user_id is null and e26.expert_user_id is null,'개별',
       if(r.expertise.scale = 2 or r.expertise.scale = 9999999 and e26.expert_user_id is not null,'카페',cast(r.expertise.scale as varchar)))) as scale,
       status,
       case when status = 20 then '정상_거래취소' else '정상' end as correct_yn
from dump_mongo_expert.remodelingstep r
left join dump.expertises e5 on r.expertuser._id = e5.expert_user_id and e5.expertise = 5
left join dump.expertises e26 on r.expertuser._id = e26.expert_user_id and e26.expertise = 26
where lower(user.name) not like '%test%'
and user.name not like '%테스트%'
and user.name not like '%효_iOS%'
and contract.inspecteddate is not null
)
select to_char(a.base_date,'yyyymm') yyyymm,
       wd.first_of_week,
       a.scale,
       a.type,
       sum(a.downpayment) gmv_org,
       sum(a.modifieddownpayment) gmv_adj,
       sum(a.downpayment + a.modifieddownpayment) as gmv,
       sum(a.contract_count) contract_count_org,
       sum(a.adj_contract_count) contract_count_adj,
       sum(a.contract_count + a.adj_contract_count) contract_count
from (
-- 계약시점을 먼저 인식
select a.dt_contract as base_date, -- 인식 기준일자
       a.dt_contract as dt_contract, -- 계약일자
       null modified_date,
       a.scale,
       a.type,
       count(distinct case when a.downpayment > 0 then a.consultation_id else null end) contract_count,
       0 adj_contract_count,
       sum(a.downpayment) as downpayment,
       0 modifieddownpayment
from raw_data a
group by 1,2,3,4,5
union all
select case when to_char(coalesce(a.modified_date,a.dt_contract),'yyyymm') <= '202212' then a.dt_contract else coalesce(a.modified_date,a.dt_contract) end as base_date, -- 보정 데이터를 인식기준일자로 인식, 22년 12월 1일 이전 데이터는 계약일자로, 이후는 수정일자로 인식
       a.dt_contract dt_contract,
       coalesce(a.modified_date,a.dt_contract) as modified_date,
       a.scale,
       a.type,
       0 contract_count,
       -count(distinct case when a.downpayment + a.modifieddownpayment = 0 then a.consultation_id else null end) adj_contract_count, -- net 0가 되면 취소처리, 단순 금액보정이면 계약건수는 둠
       0 downpayment,
       sum(a.modifieddownpayment) as modifieddownpayment
from raw_data a
where a.modifieddownpayment <> 0 --and a.modified_date <= date('2022-12-31')
group by 1,2,3,4,5
) a
left join ba_preserved.calendar wd on wd.date = a.base_date
where a.base_date < current_date and a.base_date >= cast('2021-06-01' as date)
group by 1,2,3,4
order by 1,2,3,4
;
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
;
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
