/* 
===============================================================
Stored procedure that load the files into Tables (Bronze Layer)
===============================================================

Script purpose :
Each time we Excecute the procedure the tables content truncated 
and refilled but with the new data with 'Bulk Insert'

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_batch DATETIME , @batch_end_batch DATETIME ;
	BEGIN TRY
		SET @batch_start_batch = GETDATE();
		PRINT '=======================================';
		PRINT 'Loading bronze Layer ...';
		PRINT '=======================================';

		PRINT '---------------------------------------';
		PRINT 'Loading CRM Tables...';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE CUST_INFO'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>LOADING TABLE CUST_INFO'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\DataAnalyst\SQL projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION IS :  '+ CAST(DATEDIFF(second , @start_time , @end_time )AS NVARCHAR) + 'SECONDS'
		PRINT '*********************************';
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE PRD_INFO'
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>LOADING TABLE PRD_INFO'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\DataAnalyst\SQL projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION IS :  '+ CAST(DATEDIFF(second , @start_time , @end_time )AS NVARCHAR) + 'SECONDS'
		PRINT '*********************************';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\DataAnalyst\SQL projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION IS :  '+ CAST(DATEDIFF(second , @start_time , @end_time )AS NVARCHAR) + 'SECONDS'
		PRINT '*********************************';

		PRINT '---------------------------------------';
		PRINT 'Loading ERP Tables...';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\DataAnalyst\SQL projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION IS :  '+ CAST(DATEDIFF(second , @start_time , @end_time )AS NVARCHAR) + 'SECONDS'
		PRINT '*********************************';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\DataAnalyst\SQL projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION IS :  '+ CAST(DATEDIFF(second , @start_time , @end_time )AS NVARCHAR) + 'SECONDS'
		PRINT '*********************************';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\DataAnalyst\SQL projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION IS :  '+ CAST(DATEDIFF(second , @start_time , @end_time )AS NVARCHAR) + 'SECONDS'
		PRINT '*********************************';
		SET @batch_end_batch = GETDATE();
	END TRY
	BEGIN CATCH
		PRINT '===================================='
		PRINT 'ERRO OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message : '+ ERROR_MESSAGE();
		PRINT 'Error Message : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message : ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================='
	END CATCH
END

EXEC bronze.load_bronze
