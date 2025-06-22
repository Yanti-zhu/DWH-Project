-- Good data, no cleansing required, ready to be inserted to silver

PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data Info: silver.erp_px_cat_g1v2';

insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2