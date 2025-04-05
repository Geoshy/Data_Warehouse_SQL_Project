/*
====================================================================================================================================================
Full Load (Truncate - Insert) Method:
This SQL Script is for Truncate the tables Before Bulk Insert:
====================================================================================================================================================
*/

USE DataWarehouse;
GO

-- Bulk insert table 'bronze.crm_customer_info' from csv file:
-- Before bulk insert, we will do truncate to make the table empty:
TRUNCATE TABLE bronze.crm_customer_info;
BULK INSERT bronze.crm_customer_info
FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- Bulk insert table 'bronze.crm_product_info' from csv file:
-- Before bulk insert, we will do truncate to make the table empty:
TRUNCATE TABLE bronze.crm_product_info;
BULK INSERT bronze.crm_product_info
FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- Bulk insert table 'bronze.crm_sales_details' from csv file:
-- Before bulk insert, we will do truncate to make the table empty:
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- Bulk insert table 'bronze.erp_cust_az12' from csv file:
-- Before bulk insert, we will do truncate to make the table empty:
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- Bulk insert table 'bronze.erp_loc_a101' from csv file:
-- Before bulk insert, we will do truncate to make the table empty:
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- Bulk insert table 'bronze.erp_px_cat_g1v2' from csv file:
-- Before bulk insert, we will do truncate to make the table empty:
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\IT Courses\Data Analysis Courses\Data with Baraa Data Analysis Portfolio Projects\SQL Data Warehouse Data Engineering Project\Data_Warehouse_SQL_Project\Datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
