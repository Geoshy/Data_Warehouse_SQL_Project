/*
============================================================================================================
Full Load (Truncate - Insert) Stored Procedure To Bronze Layer:
This SQL Script is to Create a Stored Procedure of Tables Bulk Insert SQL Statements To Improve Performance:
============================================================================================================
Script Purpose:
	This stored procedure 'bronze.load_bronze_sp' performs truncate Bronze layer then Bulk Insert the
	data from the source system (CRM - ERP) flat files.

Actions Performed:
	- Truncates Bronze layer tables.
	- Bulk Insert Datasets (CRM - ERP) to Bronze Layer.

Parameters:
	- None.
	- This stored procedure not accept or return any parameters.

Usage:
	EXEC bronze.load_bronze_sp;
============================================================================================================
*/

USE DataWarehouse;
GO

-- Create a 'bronze.load_bronze_sp' strored procedure:
CREATE OR ALTER PROCEDURE bronze.load_bronze_sp AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME;
	BEGIN TRY
		SET @bronze_start_time = GETDATE();
		PRINT('======================================================');
		PRINT('Loading Bronze Layer');
		PRINT('======================================================');

		PRINT('------------------------------------------------------');
		PRINT('Loading CRM System Tables:');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: bronze.crm_customer_info');
		TRUNCATE TABLE bronze.crm_customer_info;

		PRINT('>> Inserting Table: bronze.crm_customer_info');
		BULK INSERT bronze.crm_customer_info
		FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: bronze.crm_product_info');
		TRUNCATE TABLE bronze.crm_product_info;

		PRINT('>> Inserting Table: bronze.crm_product_info');
		BULK INSERT bronze.crm_product_info
		FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('>> Inserting Table: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		PRINT('------------------------------------------------------');

		PRINT('Loading ERP System Tables:');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('>> Inserting Table: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('>> Inserting Table: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('>> Inserting Table: bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		PRINT('------------------------------------------------------');
		SET @bronze_end_time = GETDATE();
		PRINT('======================================================');
		PRINT('Loading Bronze Layer is Completed')
		PRINT('Total Load Duration: ' + CAST(DATEDIFF(SECOND, @bronze_start_time, @bronze_end_time) AS NVARCHAR) + ' Seconds');
		PRINT('======================================================');

	END TRY
	BEGIN CATCH
		PRINT('Error Message: ' + ERROR_MESSAGE());
		PRINT('Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR));
		PRINT('Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('Error Procedure: ' + ERROR_PROCEDURE());
		PRINT('Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR));
		PRINT('Error State: ' + CAST(ERROR_STATE() AS NVARCHAR));
	END CATCH
END;