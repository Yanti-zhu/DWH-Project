CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Info: silver.crm_cust_info';
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
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
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

	print'==================================================='
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
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id, 
	SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key,
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	CASE UPPER(TRIM(prd_line)) 
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END as prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt ,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info;

	print'==================================================='
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
	FROM bronze.crm_sales_details;

	print'==================================================='
	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Info: silver.erp_cust_az12';
	Insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen)
	select
	case when cid like 'NAS%' THEN substring(cid, 4, len(cid))
		ELSE cid
	END AS cid,
	case when bdate > getdate () then null
		else bdate
	end as bdate,
	case when upper(trim(gen)) IN ('F', 'Female') THEN 'Female'
		 when upper(trim(gen)) IN ('M', 'Male') THEN 'Male'
		else 'n/a'
	End as gen
	from bronze.erp_cust_az12;

	print'==================================================='
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Info: silver.erp_loc_a101';
	insert into silver.erp_loc_a101(
	cid,
	cntry
	)
	select
	REPLACE(cid,'-', '') cid,
	case when trim(cntry) IN ('US', 'USA') THEN 'United States'
		 when trim(cntry) = 'DE' THEN 'Germany'
		 when trim(cntry) = '' Or cntry is null then 'n/a'
		else trim(cntry)
	END as cntry
	from bronze.erp_loc_a101 ;

	print'==================================================='
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Info: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2;

END

