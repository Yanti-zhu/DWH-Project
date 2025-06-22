-- STEP 3: AFTER bulk insert data to table 
-- We may need to Run daily if any update - THEN USE STORE PROCEDURE (SP)
-- To check the Stored Procedure - on Programmability > Stored Procedure
-- To RUN or Test Stored Procedure script: EXEC bronze.load_bronze (load_bronze is the SP name)
-- The message displayed isn't very clear, we can customaise the message

EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

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
END