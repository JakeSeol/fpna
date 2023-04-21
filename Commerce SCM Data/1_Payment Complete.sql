-- 결제완료 기준 할인내역
-- 23/02/24 Stage schema 상품쿠폰 금액 반영 60~89행
with summary as (
select cast(cast(greatest(a.base_dttm,o.payment_at) as date) as varchar) base_dt,
       case when a.batch_job = 'manual_correction'  then '할인금액보정'
            when cast(a.base_dttm as date) <= cast(o.payment_at as date) then '결제완료'
            when a.kind = -1 then '결제완료후취소'
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
        sum(case when cast(a.base_dttm as date) <= cast(o.payment_at as date) then c."count" else 0 end) "수량"
from finance.fin_coupon_order_options_settlement a
inner join dump.order_options c on c.id = cast(a.order_option_id as integer) and c.status IN (1,2,3,4,5,6,7)
inner join dump.order_productions b on b.id = c.order_production_id
inner join dump.orders o on o.id = b.order_id and o.payment_at is not null
where b.seller_id = 476599
  and cast(greatest(a.base_dttm,o.payment_at) as date) >= cast('2022-07-01' as date)
  and cast(greatest(a.base_dttm,o.payment_at) as date) <  cast('2022-08-01' as date)
group by 1,2,3,4,5,6,7,8,9,10,11,12
having sum(a.coupon_cost) <> 0
union all
select cast(cast(greatest(a.base_dttm,o.payment_at) as date) as varchar) base_dt,
       case when a.batch_job = 'manual_correction'  then '할인금액보정'
            when cast(a.base_dttm as date) <= cast(o.payment_at as date) then '결제완료'
            when a.kind = -1 then '결제완료후취소'
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
        sum(case when cast(a.base_dttm as date) <= cast(o.payment_at as date) then c."count" else 0 end) "수량"
from finance.fin_mileage_order_options_settlement a
inner join dump.order_options c on c.id = cast(a.order_option_id as integer) and c.status IN (1,2,3,4,5,6,7)
inner join dump.order_productions b on b.id = c.order_production_id
inner join dump.orders o on o.id = b.order_id and o.payment_at is not null
where b.seller_id = 476599
  and cast(greatest(a.base_dttm,o.payment_at) as date) >= cast('2022-07-01' as date)
  and cast(greatest(a.base_dttm,o.payment_at) as date) <  cast('2022-08-01' as date)
group by 1,2,3,4,5,6,7,8,9,10,11,12
having sum(a.mileage_cost) <> 0
union all
-- Stage schema 추가
select cast(cast(pt.transaction_at as date) as varchar) base_dt,
       case when pt.transaction_type = 'PAYMENT' then '결제완료'
            when pt.transaction_type = 'CANCEL' then '결제취소' end "정산구분",
       coalesce(op.seller_id,selling_company_id) seller_id,
       pt.order_id,
       cast(oo.id as varchar) order_option_id,
       op.id production_id,
       oo.reference_option_id,
       coalesce(op.name,'배송비') "상품명",
       oo.explain "옵션명",
       to_char(pt.transaction_at,'yyyy-mm-dd') "결제완료일",
       to_char(oo.delivery_complete_date, 'yyyy-mm-dd') "배송완료일",
       to_char(oo.purchase_confirm_date, 'yyyy-mm-dd') "구매확정일",
       -sum(case when pia.payment_method_type in ('PRODUCT_COUPON') then pia.amount else 0 end) "쿠폰비용 총액",
       0 "포인트 총액",
       0 "수량"
from dump_payment_stage.payment_transactions pt
left join dump_payment_stage.payment_transaction_items pti on pti.payment_id = pt.payment_id and pti.transaction_id = pt.transaction_id
left join dump_payment_stage.payment_items pi on pi.payment_item_id = pti.payment_item_id
left join dump_payment_stage.payment_transaction_item_amounts pia on pia.payment_item_id = pti.payment_item_id and pia.transaction_id = pt.transaction_id
left join dump_stage.order_options oo on cast(oo.id as varchar) = pi.item_reference_id --and oo.status IN (1,2,3,4,5,6,7)
left join dump_stage.order_productions op on op.id = oo.order_production_id
where 1=1
  and pt.service_id = 'OHOUSE'
  and op.seller_id = 6824664
  and cast(pt.transaction_at as date) >= cast('2023-01-01' as date)
  and cast(pt.transaction_at as date)  < cast('2023-02-01' as date)
group by 1,2,3,4,5,6,7,8,9,10,11,12
having sum(case when pia.payment_method_type in ('PRODUCT_COUPON') then pia.amount else 0 end) <> 0
)
select substring(a.base_dt,1,4) "결제연도",
       substring(a.base_dt,6,2) "결제월",
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
       a.base_dt "결제/결제취소일"
from summary a
group by 1,2,3,4,5,6,7,9,10,13
