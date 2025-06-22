-- DATA CHECK AFTER DATA CLEANSE and DATA STANDARDISATION
-- CHANGE BRONZE TO SILVER table

-- Check for Nulls or Dupes in Primary Key
-- Expectation: No Result

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_ID IS NULL

-- Check for unwanted spaces
-- Expectation: No Results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Data Standardisation & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT* FROM silver.crm_cust_info

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
-- If business allow - replace Null with O
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Check invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info

-- Check for invalid Date orders, prd_start_date > prd_end_date
-- Cant swap the date, because there are multiple start and end dates for 1 product
-- solution: end date is from the next start date -1
-- the record is null end date 
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS prd_end_dt_test -- add -1 so there's no overlap between start date and end date
FROM bronze.crm_prd_info
WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- DATA CHECK AND VALIDATION FOR CRM SALES DETAILS TABLE !!!!!!!!!!!!!!!!!!!!!!!!
-- Check for invalid dates

Select
nullif(sls_order_dt,0) sls_order_dt
From bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 0
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101


-- check data consistency between sales, quantity and price
-- Sales = Quantity * price
-- Values must not be NULL, zero or Negative
/* Agreed Rules for Solution:
>> If sales is negative, zero or null, derive it using Quantity and price.
>> if Price is zero or null, calculate it using Sales and Quantity.
>> if Price is negative, convert it to a positive value.
SOLUTION USING CASE WHEN BELOW
*/

select distinct
sls_sales,
sls_quantity,
sls_price,

CASE WHEN sls_sales is null or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEn sls_quantity * ABS (sls_price)
Else sls_sales
END as sls_sales,

case when sls_price is null or sls_price <= 0
	then sls_sales / Nullif(sls_quantity, 0)
	else sls_price
end as sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL or sls_price IS NULL
OR sls_sales <= 0 or sls_quantity <= 0 OR sls_price <= 0
GROUP By sls_sales, sls_quantity, sls_price