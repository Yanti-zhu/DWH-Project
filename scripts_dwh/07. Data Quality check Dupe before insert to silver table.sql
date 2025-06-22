-- Before inserting data to Silver table, check for data quality 
-- Check for Nulls or Duplicates in Primary Key, Expectation: No Result

SELECT *
FROM bronze.crm_cust_info;

-- check for Duplicates in Primary Key:
SELECT
cst_id,
COUNT(*) as appearance
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Investigate the record with issues, eg check create date 
-- Rank the result to pick up the newest one to keep, then sort data by create date DESC 
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Remove the WHERE filter to change everything on the flag_last
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info;

-- To Double check the query, user the above script as a sub query to see all we don't need
-- use the flag_last = 1, to keep the clean no duplicate value that we want

SELECT
*
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
)t WHERE flag_last !=1;