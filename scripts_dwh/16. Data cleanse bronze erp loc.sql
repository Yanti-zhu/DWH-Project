-- Check the data in erp_loc_a101, we have AW-xxxxx, we want to remove the '-'

select
cid,
cntry
from bronze.erp_loc_a101;

select
REPLACE(cid,'-', '') cid,
case when trim(cntry) IN ('US', 'USA') THEN 'United States'
	 when trim(cntry) = 'DE' THEN 'Germany'
	 when trim(cntry) = '' Or cntry is null then 'n/a'
	else trim(cntry)
END as cntry
from bronze.erp_loc_a101 
Where REPLACE(cid,'-', '') NOT IN
(select cst_key from silver.crm_cust_info) -- check for unmatching data with crm cust info for join later), return nothing is expected

-- Data standardisation & consistency

select distinct cntry
from bronze.erp_loc_a101
order by cntry

select distinct
REPLACE(cid,'-', '') cid,
cntry,
case when trim(cntry) IN ('US', 'USA') THEN 'United States'
	 when trim(cntry) = 'DE' THEN 'Germany'
	 when trim(cntry) = '' Or cntry is null then 'n/a'
	else trim(cntry)
END as cntry
from bronze.erp_loc_a101 
