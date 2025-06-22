PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting Data Info: silver.crm_prd_info';

INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT -- THIS IS THE FINAL SCRIPT FROM FILE #10
prd_id,
--prd_key,
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