-- Check for unwanted Spaces before inserting to Silver table
-- Expecation: No Results
-- Do this check for lastname column as well and all string values on the table

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- CLEAN UP UNWANTED space

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- DATA Standardisation & Consistency 
-- See what values are in the column

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- We don't want to use abbreviation in the data , eg M = Male, F = Female
-- CHECK the cst_create_date Datatype is Date not a String

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
-- cst_marital_status, removes this the same as cst_gndr
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- add TRIM just in case any unwanted space in the data
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
-- cst_gndr, -- remove this old initial column as we have the new one below, apply UPPER() just in case mixed-case values appear eg 'M' or 'm'
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- add TRIM just in case any unwanted space in the data
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- NEXT is to write the INSERT STATEMENT
-- USE THE LAST QUERY in data standardisation, and ADD INSERT INTO statement


INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
-- cst_marital_status, removes this the same as cst_gndr
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- add TRIM just in case any unwanted space in the data
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
-- cst_gndr, -- remove this old initial column as we have the new one below, apply UPPER() just in case mixed-case values appear eg 'M' or 'm'
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- add TRIM just in case any unwanted space in the data
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;