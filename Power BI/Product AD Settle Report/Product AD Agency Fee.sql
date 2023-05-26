-- 2023/05/22 초안 리포트 생성
-- 2023/05/25 Temp 테이블 스키마 변경 (temp.seller_agency_code_mapping->finance.fin_seller_agency_code_mapping)
-- 2023/05/26 조회기간을 작년 1월 1일부터로 변경
-- Product AD Agency Fee calculation for Power BI
with raw_data as (
-- 상품광고 매출을 계산
select al.ledgertype -- 클릭차감 / 광고비 충전의 구분하는 type 값
    , cast(al.salesuserid as integer) salesuserid -- 판매자 id
    , from_unixtime(al.clicktimestamp / 1000, 'Asia/Seoul') click_date --클릭시점
    , from_unixtime(al.ledgertimestamp / 1000, 'Asia/Seoul') ledger_date --데이터 기록된 시점
    , cast(cost.cpc as integer) as cost_cpc -- 실제 적용된 cpc
    , cast(cost.vat as integer) as cost_vat -- 실제 적용된 vat
    , cast(cost.ispaid as varchar) as cost_ispaid --광고비의 유상 / 무상 구분 (true면 유상, false면 무상)
    , cast(request.member1.campaignid as varchar) as campaignid -- 설정된 캠페인 id
    , cast(request.member1.cpc as integer) as request_cpc -- 몰로코 to 버킷으로 전달된 cpc 값. 이걸 다 합하면 말씀하신 광고 총 매출이 됩니다.
    , wd.date
    , to_char(wd."date",'yyyymm') yyyymm
from advertise."ad-ledger" al
inner join ba_preserved.calendar wd             on to_char(wd.date,'yyyymmdd') = concat(al."year", al."month", al."day")
where al.ledgertype = 'CLICK'
  and al.budget is not null
  and al.cost.ispaid is not null
  and wd."date" >= date_trunc('year',current_date) - interval '1' year
--   and to_char(wd."date",'yyyymm') = '202304'
)
,agency_raw as (
-- Agency Mapping raw History 데이터를 구조화
select st.seller_id,
       st.agency,
       st.dt start_dt,
       ed.dt end_dt,
       row_number () over ( partition by st.seller_id,st.agency,st.dt order by ed.dt) rank
from finance.fin_seller_agency_code_mapping st
left join finance.fin_seller_agency_code_mapping ed on ed.type = '0' and ed.seller_id = st.seller_id and ed.agency = st.agency and st.dt < ed.dt -- and ed.deleted_status = '0'
where st.type = '1'
--   and st.deleted_status = '0'
)
,agency_map as (
-- Agency Mapping raw 테이블을 from ~ to 의 형태로 변경
select a.seller_id,
       a.agency,
       cast(a.start_dt as date) start_dt,
       cast(coalesce(a.end_dt,'2999-12-31') as date) - interval '1' day end_dt
from agency_raw a
where a.rank = 1 )
select al.yyyymm,
       al.date dt,
       al.salesuserid sales_user_id,
       coalesce(am.agency,'self') agency,
       concat(cast(am.start_dt as varchar),'~',cast(am.end_dt as varchar)) agency_period,
       sum(if(al.cost_ispaid = 'true', al.cost_cpc + al.cost_vat, 0)) as paid_incl_vat,
       sum(if(al.cost_ispaid = 'false', al.cost_cpc + al.cost_vat, 0)) as free_incl_vat,
       round(sum(if(am.agency is not null and al.cost_ispaid = 'true', al.cost_cpc + al.cost_vat, 0)) * 0.1*100/110,0) as agency_fee
from raw_data al
left join agency_map am on cast(am.seller_id as integer) = al.salesuserid and al."date" between am.start_dt and am.end_dt
group by 1,2,3,4,5