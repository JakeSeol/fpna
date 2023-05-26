-- 2023/05/22 초안 리포트 생성
-- Product AD Sales User Information
select e.sales_user_id,
       u.id user_id,
       su.company salesuser_company,
       u.nickname salesuser_nickname,
       concat(su.license1, '-', su.license2, '-', su.license3) salesuser_biz_number,
       concat(e.ad_manager_name,'(',e.ad_manager_email,')') ad_manager,
       concat(e.ad_manager_phone1, '-', e.ad_manager_phone2, '-', e.ad_manager_phone3) ad_manager_phone,
       concat(e.settlement_manager_name,'(',e.settlement_manager_email,')') ad_settle_manager,
       concat(e.settlement_manager_phone1, '-', e.settlement_manager_phone2, '-', e.settlement_manager_phone3) ad_settle_manager_pone
from  dump_freq.ad_user_properties_view e
left join dump.sales_users su               on e.sales_user_id = su.id
left join dump.users u                      on e.sales_user_id = u.userable_id and u.userable_type = 'SalesUser'
group by 1,2,3,4,5,6,7,8,9
