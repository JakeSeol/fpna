-- 2023/05/22 Draft 작성
-- 2023/05/25 Temp 테이블 스키마 변경 (temp.seller_agency_code_mapping->finance.fin_seller_agency_code_mapping)
-- Product AD Sales User - Agency Mapping Error Validation for Power BI
with agency_raw as (
select st.seller_id,
       st.agency,
       st.dt start_dt,
       ed.dt end_dt,
       row_number () over ( partition by st.seller_id,st.agency,st.dt order by ed.dt) rank
from finance.fin_seller_agency_code_mapping st
left join finance.fin_seller_agency_code_mapping ed on ed.type = '0' and ed.seller_id = st.seller_id and ed.agency = st.agency and st.dt < ed.dt --and ed.deleted_status = '0'
where st.type = '1'
--   and st.deleted_status = '0'
    )
,agency_map as (
select a.seller_id,
       a.agency,
       cast(a.start_dt as date) start_dt,
       cast(coalesce(a.end_dt,'3000-01-01') as date) - interval '1' day end_dt
from agency_raw a
where a.rank = 1 )
,errors as (
select am.seller_id,
       cast(wd.date as varchar) date,
       count(am.agency) raw_count
from agency_map am
inner join ba_preserved.calendar wd on wd.date between am.start_dt and am.end_dt
group by 1,2
having count(am.agency) > 1 )
select am.agency,
       am.seller_id,
       am.start_dt,
       am.end_dt,
       concat(min(er."date"),'~',max(er."date")) error_period
from agency_map am
inner join errors er on er.seller_id = am.seller_id
group by 1,2,3,4