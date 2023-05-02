-- 23/03/06 : raw data validation 단계 생성
-- 23/04/24 : point_mapping 테이블 추가
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
union all
select 'finance.mileage_point_type_mapping' table_name, p.category_type join_key, count(p.category_type) row_count
from dump_point.points p left join finance.mileage_point_type_mapping pm on pm.category_type = p.category_type
where pm.category_type is null group by 1,2