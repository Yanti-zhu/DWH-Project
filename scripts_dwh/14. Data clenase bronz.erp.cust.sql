-- Data cleanse and validation for erp_cust_az12

select
cid,
bdate,
gen
from bronze.erp_cust_az12
where cid like '%AW00011000%' -- after checkin, so we want to remove the NAS

select * from silver.crm_cust_info;

-- removing NAS from 'cid'
select
case when cid like 'NAS%' THEN substring(cid, 4, len(cid))
	ELSE cid
END AS cid,
bdate,
gen
from bronze.erp_cust_az12
-- add the following to check if there's no unmatching data with cust info table
--where case when cid like 'NAS%' THEN substring(cid, 4, len(cid)) 
	--ELSE cid
--end not in (select distinct cst_key from silver.crm_cust_info)

-- Identify Out of Range Dates
select distinct 
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()