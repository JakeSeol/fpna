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
