/*	
==================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================
Script Purpose:
    This script defines and executes a stored procedure called "bronze.load_bronze".
    This procedure loads raw data (CSV files) into a "Bronze Layer" of a data warehouse.

In a data lakehouse/ warehouse architecture:
-> Bronze Layer = raw data (as ingested from source systems like CRM, ERP, ...)
-> Silver Layer = cleaned standardized data.
-> Gold layer = business-rady data (aggregations, KPIs, dashboards)
So, this script is the ingestions pipeline: pulling CVs intso SQL tables.

  What happens inside the script?

  1. Prints messages to show progress.
  2. Truncates tables (empties them completely)
	- TRUNCATE TABLE is faster than DELETE and resets identity columns
	- This means it always does a full reload (no incremental updates)
  3. Bulk inserts CSV data into tables:
	- Uses BULK INSERT to quickly load large amounts of data
	- Options:
		- FIRSTROW = 2 --> skip the header row in the CSV
		- FIELDTERMINATOR = ',' --> comma-separated file
		- TABLOCK --> lock table during insert (faster)

  Tables Loaded:
  	From CRM system:
  		-	bronze.crm_cust_info ← cust_info.csv
  		-	bronze.crm_prd_info ← prd_info.csv
  		-	bronze.crm_sales_details ← sales_details.csv
  
  	From ERP system:
  		-	bronze.erp_cust_az12 ← CUST_AZ12.csv
  		-	bronze.erp_loc_a101 ← LOC_A101.csv
  		-	bronze.erp_px_cat_g1v2 ← PX_CAT_G1V2.csv
  
  Error Handling, if something goes wrong:
  
  PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
  PRINT 'Error Message' + ERROR_MESSAGE();
  PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
  	-	ERROR_MESSAGE() gives the text of the error.
  	-	ERROR_NUMBER() gives the error code.
  	-	Useful for debugging if a file path is wrong, CSV is corrupted, or table doesn’t exist.
  
  Usage Exmaple:
    EXEC bronze.load_bronze;
==================================================

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '=============================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================================';

		PRINT '-------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; 
	
		PRINT '>>Inserting Data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ewald\Desktop\Road2DataAnalyst\Data Engineering\Projects\Data Warehouse from Scratch - Engineering\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds '
		PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>Inserting Data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ewald\Desktop\Road2DataAnalyst\Data Engineering\Projects\Data Warehouse from Scratch - Engineering\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds '
		PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ewald\Desktop\Road2DataAnalyst\Data Engineering\Projects\Data Warehouse from Scratch - Engineering\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds '
		PRINT '>> ---------------';

		PRINT '-------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ewald\Desktop\Road2DataAnalyst\Data Engineering\Projects\Data Warehouse from Scratch - Engineering\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds '
		PRINT '>> ---------------';


		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ewald\Desktop\Road2DataAnalyst\Data Engineering\Projects\Data Warehouse from Scratch - Engineering\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds '
		PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ewald\Desktop\Road2DataAnalyst\Data Engineering\Projects\Data Warehouse from Scratch - Engineering\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds '
		PRINT '>> ---------------';

		SET @batch_end_time = GETDATE();
		PRINT '===================================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================';
		END TRY
		BEGIN CATCH
			PRINT '===============================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '===============================================';
		END CATCH
END
