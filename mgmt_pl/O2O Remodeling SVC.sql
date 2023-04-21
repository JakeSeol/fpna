-- 22/12/14 : O2O 시공 GMV/계약건수 데이터 추가
-- 23/01/16 : O2O Remodeling GMV 쿼리 수정(21년 6월부터 집계)
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
