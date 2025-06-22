-- AFTER joining table, check if any dupes were introduced by the join logic, to check, use GROUP BY
-- then subquery the initial join statement

SELECT cst_id, COUNT(*) FROM
	(SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
) t GROUP BY cst_id
HAVING COUNT(*) > 1;

PRINT '----------------------------------------------------------------------'
-- CHECK DATA of JOIN (this is the main query)
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_gndr,
	ci.cst_marital_status,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid

-- We would notice there are 2 gender columns, do data check on this part
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY ci.cst_gndr,ca.gen

-- Scenario: business decided that the master data is CRM info. so go with CRM data if there's unmatch.
-- Once this is done, take the query back to the main query (the data integration part ie, CASE WHEN...)

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
	ELSE COALESCE(ca.gen, 'n/a') -- use COALESCE to replace NULL with n/a
END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY ci.cst_gndr,ca.gen

-- THIS IS MAIN QUERY + DATA INTEGRATION + RENAME for Business
-- Considered as a dimension table because it contains description no transaction etc
-- If no Primary key, create Surrogate Key as unique identifier, in this case, we use row_number function
-- Create object as VIEW for Gold layer (as per dwh architecture)
-- The final script to create VIEW for gold layer is copied as file #

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- this is the surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry as country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen, 'n/a') -- use COALESCE to replace NULL with n/a
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date	
	FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid