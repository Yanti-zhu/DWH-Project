-- We didnt change anything in DDL eg, datatype or adding new column, so we dont need to update the DDL script
-- We can insert this cleaned statement into silver table
-- if need to delete all data and re-insert: TRUNCATE TABLE silver.erp_cust_az12;

PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data Info: silver.erp_cust_az12';

Insert into silver.erp_cust_az12(
cid,
bdate,
gen)
select
case when cid like 'NAS%' THEN substring(cid, 4, len(cid))
	ELSE cid
END AS cid,
case when bdate > getdate () then null
	else bdate
end as bdate,
case when upper(trim(gen)) IN ('F', 'Female') THEN 'Female'
	 when upper(trim(gen)) IN ('M', 'Male') THEN 'Male'
	else 'n/a'
End as gen
from bronze.erp_cust_az12