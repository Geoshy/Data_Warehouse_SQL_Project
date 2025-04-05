/*
===================================================================================================================
This SQL Script is for Create the Structure of Tables of Silver Layer of 'DataWarehouse' DW:
Note that Before Create the Tables Structures We Drop the Tables If Exist Using (OBJECT_ID ('Table_Name', 'U')).
===================================================================================================================
*/

USE DataWarehouse;
GO
-- Create table 'silver.crm_customer_info':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('silver.crm_customer_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_customer_info;
CREATE TABLE silver.crm_customer_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(30),
	cst_lastname NVARCHAR(30),
	cst_marital_status NVARCHAR(20),
	cst_gndr NVARCHAR(20),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Create table 'silver.crm_product_info':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('silver.crm_product_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_product_info;
CREATE TABLE silver.crm_product_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(70),
	prd_cost DECIMAL(6,2),
	prd_line NVARCHAR(20),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Create table 'silver.crm_sales_details':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales DECIMAL(6,2),
	sls_quantity INT,
	sls_price DECIMAL(6,2),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Create table 'silver.erp_cust_az12':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(20),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Create table 'silver.erp_loc_a101':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Create table 'silver.erp_px_cat_g1v2':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(60),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

