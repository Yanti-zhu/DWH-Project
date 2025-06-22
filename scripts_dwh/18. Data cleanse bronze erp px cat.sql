-- Data check table bronze.px_cat_g1v2 before inserting to silver
-- all data is good in this table and clean, 
-- Next is insert this to silver

select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

--check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

-- data standardisation & consistency, do this with all column
select distinct
cat from bronze.erp_px_cat_g1v2