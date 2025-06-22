-- To Customise the messages after SP run
-- To handle/ debug error during execution, use BEGIN TRY and END TRY
-- Track ETL duration to load each table
-- Track ETL duration to load the Bronze layer

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY -- this is to catch/ debug error during execution
		SET @batch_start_time = GETDATE();
		PRINT '==============================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================================================';

		PRINT '------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- this is to empty the table

		-- Reload again:
		PRINT '>> Inserting data into: bronze.crm_cust_info';
		BULK  INSERT bronze.crm_cust_info
		FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		-- use CAST because the ouput of is number so we convert to nvarchar value
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into: bronze.crm_prd_info';
		BULK  INSERT bronze.crm_prd_info
		FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into: bronze.crm_sales_details';
		BULK  INSERT bronze.crm_sales_details
		FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		PRINT '------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into: bronze.erp_cust_az12';
		BULK  INSERT bronze.erp_cust_az12
		FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into: bronze.erp_loc_a101';
		BULK  INSERT bronze.erp_loc_a101
		FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
		BULK  INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\z3166315\OneDrive - UNSW\Documents\zYanti\06. sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================================='
		PRINT 'Loading Bronze Layer is Completed'
		PRINT '		- Total Load Duration: ' + Cast(datediff(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================='
		END TRY -- this is the end try for capturing error during execution
	BEGIN CATCH -- to capture the error, this will be executed only if SQL failed the execution (the TRY statement)
				-- video ref: 1:00:58:35
		PRINT '======================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + Error_Message ();
		PRINT 'Error Message' + Cast(error_number() AS NVARCHAR);
		PRINT 'Error Message' + Cast(error_state() AS NVARCHAR);
		PRINT '======================================================================='
	END CATCH	-- the end script to capture error
END