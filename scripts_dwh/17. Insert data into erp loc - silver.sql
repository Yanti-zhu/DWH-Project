-- Cleansed statement ready to insert data into silver table

PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting Data Info: silver.erp_loc_a101';

insert into silver.erp_loc_a101(
cid,
cntry
)
select
REPLACE(cid,'-', '') cid,
case when trim(cntry) IN ('US', 'USA') THEN 'United States'
	 when trim(cntry) = 'DE' THEN 'Germany'
	 when trim(cntry) = '' Or cntry is null then 'n/a'
	else trim(cntry)
END as cntry
from bronze.erp_loc_a101 

