-- ignore 1st row as it is the column header
-- seperator between fields
-- IF accidently loaded the data 2x, THEN Truncate the table the load again


TRUNCATE TABLE bronze.crm_cust_info; -- this is to empty the table
-- Reload again:
BULK  INSERT bronze.crm_cust_info
FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.crm_prd_info;
BULK  INSERT bronze.crm_prd_info
FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.crm_sales_details;
BULK  INSERT bronze.crm_sales_details
FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_cust_az12;
BULK  INSERT bronze.erp_cust_az12
FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_loc_a101;
BULK  INSERT bronze.erp_loc_a101
FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK  INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);