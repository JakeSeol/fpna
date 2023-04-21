-- 22/12/14 : 1.포인트 코드 변경 (구매대가 1,171 -> 1만으로)
-- 23/03/06 : Point 쿼리 일부 수정 (cast as varchar)
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