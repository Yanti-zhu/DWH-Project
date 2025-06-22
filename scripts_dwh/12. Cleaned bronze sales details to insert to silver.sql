-- THIS IS CLEANED SALES DETAILS TABLE READY TO BE INSERTED 
-- NEXT GO TO THE DDL - WHERE WE CREATE TABLE, CHECK THE DDL #FILE 06 (COLUMNS FOR TABLES ENSURE THEY MATCH AND DATA TYPE MATCH)
-- COPY THE FOLLOWING SCRIPT, AND ADD INSERT TABLE , NEXT STEP FILE #13 INSERT TO SILVER TABLE
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
END AS sls_due_dt,
CASE WHEN sls_sales is null or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEn sls_quantity * ABS (sls_price)
Else sls_sales
END as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0
	then sls_sales / Nullif(sls_quantity, 0)
	else sls_price
end as sls_price
FROM bronze.crm_sales_details