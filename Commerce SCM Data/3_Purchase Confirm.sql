-- 구매확정기준 할인내역
-- 23/02/24 Stage schema 상품쿠폰 금액 반영 64~85행
with  max_base_dt as (
select max(os.base_dt) max_base_dt
from dump_settlement_stage.order_sales_l os
)
,summary as (
select cast(cast(greatest(a.base_dttm,c.purchase_confirm_date) as date) as varchar) base_dt,
       case when a.batch_job = 'manual_correction'  then '할인금액보정'
            when cast(a.base_dttm as date) <= cast(c.purchase_confirm_date as date) then '구매확정'
            when a.kind = -1 then '구매확정후취소'
            else '할인금액재계산' end "정산구분",
       b.seller_id,
       a.order_id,
       a.order_option_id,
       b.production_id,
       c.reference_option_id,
       b.name "상품명",
       c.explain "옵션명",
       to_char(o.payment_at,'yyyy-mm-dd') "결제완료일",
       to_char(c.delivery_complete_date, 'yyyy-mm-dd') "배송완료일",
       to_char(c.purchase_confirm_date, 'yyyy-mm-dd') "구매확정일",
       -sum(a.coupon_cost) "쿠폰비용총액",
       0 "포인트총액",
        sum(case when cast(a.base_dttm as date) <= cast(c.purchase_confirm_date as date) then c."count" else 0 end) "수량"
from finance.fin_coupon_cart_order_options_settlement a
inner join dump.order_options c on c.id = cast(a.order_option_id as integer) and c.purchase_confirm_date is not null
inner join dump.order_productions b on b.id = c.order_production_id
inner join dump.orders o on o.id = b.order_id
where b.seller_id = 476599
  and cast(greatest(a.base_dttm,c.purchase_confirm_date) as date) >= cast('2022-07-01' as date)
  and cast(greatest(a.base_dttm,c.purchase_confirm_date) as date) <  cast('2022-08-01' as date)
group by 1,2,3,4,5,6,7,8,9,10,11,12
having sum(a.coupon_cost) <> 0
union all
select cast(cast(greatest(a.base_dttm,c.purchase_confirm_date) as date) as varchar)  base_dt,
       case when a.batch_job = 'manual_correction'  then '할인금액보정'
            when cast(a.base_dttm as date) <= cast(c.purchase_confirm_date as date) then '구매확정'
            when a.kind = -1 then '구매확정후취소'
            else '할인금액재계산' end "정산구분",
       b.seller_id,
       a.order_id,
       a.order_option_id,
       b.production_id,
       c.reference_option_id,
       b.name "상품명",
       c.explain "옵션명",
       to_char(o.payment_at,'yyyy-mm-dd') "결제완료일",
       to_char(c.delivery_complete_date, 'yyyy-mm-dd') "배송완료일",
       to_char(c.purchase_confirm_date, 'yyyy-mm-dd') "구매확정일",
       0 "쿠폰비용총액",
       -sum(a.mileage_cost) "포인트총액",
        sum(case when cast(a.base_dttm as date) <= cast(c.purchase_confirm_date as date) then c."count" else 0 end) "수량"
from finance.fin_mileage_order_options_settlement a
inner join dump.order_options c on c.id = cast(a.order_option_id as integer) and c.purchase_confirm_date is not null
inner join dump.order_productions b on b.id = c.order_production_id
inner join dump.orders o on o.id = b.order_id
where b.seller_id = 476599
  and cast(greatest(a.base_dttm,c.purchase_confirm_date) as date) >= cast('2022-07-01' as date)
  and cast(greatest(a.base_dttm,c.purchase_confirm_date) as date) <  cast('2022-08-01' as date)
group by 1,2,3,4,5,6,7,8,9,10,11,12
having sum(a.mileage_cost) <> 0
union all
-- stage 스키마 쿼리
select to_char(os.settlement_at,'yyyy') "정산연도",
       to_char(os.settlement_at,'mm') "정산월",
       case when to_char(os.settlement_at,'dd') <= '14' then '2차' else '1차' end "정산차수",
       case when os.recognized_type = 'PURCHASE_CONFIRMED' then '구매확정' else '구매확정후취소' end "정산구분",
       os.order_id "주문번호",
       os.order_option_id "주문옵션번호",
       os.product_name "상품명",
       os.option_name "옵션명",
       os.quantity "수량",
       os.product_id "상품번호",
       os.option_id "상품옵션번호",
       0 "포인트총액",
       sum(dpd.amount) "쿠폰비용총액",
       cast(os.recognized_at as date) "구매확정/취소일"
from dump_settlement_stage.order_sales_l os
inner join max_base_dt mbd on mbd.max_base_dt = os.base_dt
inner join dump_settlement_stage.discount_price_detail_l dpd on dpd.base_dt = os.base_dt and dpd.order_sales_id = os.id and dpd.discount_type = 'PRODUCT_COUPON'
where os.recognized_at >= cast('2023-01-01' as date)
  and os.recognized_at  < cast('2023-02-01' as date)
  and os.seller_id = 4202491 -- 4807902
group by 1,2,3,4,5,6,7,8,9,10,11,12,14
)
select to_char(case when substring(a.base_dt,9,2) <= '14' then date(a.base_dt) else date_add('month', 1, date(a.base_dt)) end,'yyyy') "정산연도",
       to_char(case when substring(a.base_dt,9,2) <= '14' then date(a.base_dt) else date_add('month', 1, date(a.base_dt)) end,'mm') "정산월",
       case when substring(a.base_dt,9,2) <= '14' then '2차' else '1차' end "정산차수",
       a."정산구분" "정산구분",
       a.order_id "주문번호",
       a.order_option_id "주문옵션번호",
       "상품명",
       "옵션명",
       sum(coalesce(a."수량",0)) "수량",
       a.production_id "상품번호",
       a.reference_option_id "상품옵션번호",
       cast(sum(coalesce(a."포인트총액",0)) as bigint) "포인트총액",
       cast(sum(coalesce(a."쿠폰비용총액",0)) as bigint) "쿠폰비용총액",
       a.base_dt "구매확정/취소일"
from summary a
group by 1,2,3,4,5,6,7,8,10,11,14
;
