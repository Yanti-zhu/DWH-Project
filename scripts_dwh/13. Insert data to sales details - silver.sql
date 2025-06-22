-- INSERT TO SILVER TABLE ADD THE LAST CLEANSED SCRIPT WITH THE INSERT INTO STATEMENT
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Info: silver.crm_sales_details';

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price 
)
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