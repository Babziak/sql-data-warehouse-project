/*
STORED PROCEDURE: LOAD BRONZE LAYER
*/

--execute bronze.load_layer

CREATE OR ALTER PROCEDURE bronze.load_layer AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT'================================';
		PRINT'Loading bronze layer';
		PRINT'================================';

		PRINT'---------------------------------';
		PRINT'Loading CRM TABLES';
		PRINT'---------------------------------';

		SET @start_time = GETDATE();
		PRINT'>>Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT'>>Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		
		SET @start_time = GETDATE();
		PRINT'>>Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT'>>Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT'>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' :seconds';

		SET @start_time = GETDATE();
		PRINT'>>Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>>Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' :seconds';

		PRINT'---------------------------------';
		PRINT'Loading ERP TABLES';
		PRINT'---------------------------------';
		
		SET @start_time = GETDATE();
		PRINT'>>Truncating table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12

		PRINT'>>Inserting data into: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' :seconds';

		SET @start_time = GETDATE();
		PRINT'>>Truncating table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101

		PRINT'>>Inserting data into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT'>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' :seconds';

		SET @start_time = GETDATE();
		PRINT'>>Truncating table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2

		PRINT'>>Inserting data into: bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>>Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' :seconds';
	END TRY
	BEGIN CATCH
		PRINT'============================='
		PRINT'ERROR OCCURED DURING LOADIN BRONZE LAYER'
		PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'============================='
	END CATCH
END
