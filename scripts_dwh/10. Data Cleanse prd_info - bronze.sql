-- THIS IS THE STEP OF CHECKING BRONZE TABLE, CLEANSE AND MAKE SURE IT IS CLEANSED AND CAN BE INSERTED TO SILVER TABLE - ONCE ALL STEPS COMPLETED, USE THE
-- SCRIPT TO INSERT DATA TO SILVER TABLE, NEXT STEP IS CHECK FILE #06 ADD NEW COLUMN INTO THE DDL THEN ACTION FILE # 11 IN THIS FOLDER
-- Check column to join with erp later
-- unmatch data in cat_id, between '-' from crm vs '_' from erp
-- We extract the 2nd part of prd_key because we need to join it with another table , ie sales_details
-- Replace Null with 0 in the prd_cost


SELECT
prd_id,
prd_key,
--SUBSTRING(prd_key,1,5) as cat_id, > extract this is to join with category id later, 1 is the position where you want to start the extract 
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- user replace to replace '-' with '_'
SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key, -- use LEN to make it DYNAMIC because we don't know the exact number of the prd_key
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
-- prd_line, REPLACE abbreviation with business value, use CASE WHEN
CASE UPPER(TRIM(prd_line)) -- use this method so we dont need to keep repeating the upper trim on each When clause
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END as prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt ,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info


-- ADD the following statement to check if there's anything unmatch
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN -- this is to check the new cat_id in CRM that doesnt match with ERP
(SELECT distinct id from bronze.erp_px_cat_g1v2)

WHERE SUBSTRING(prd_key,7, LEN(prd_key)) IN(
(SELECT sls_prd_key FROM bronze.crm_sales_details WHERE sls_prd_key LIKE 'FK-1%') -- to check if there's no match


SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
