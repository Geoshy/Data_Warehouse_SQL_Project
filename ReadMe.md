# **Data Warehouse SQL Project:**

## **1. Introduction:**
### **Data Warehouse Project Overview:**

In this project, we aim to create a robust data warehouse to organize, structure, and prepare data from two critical company systems, CRM (Customer Relationship Management) and ERP (Enterprise Resource Planning). The goal is to establish a comprehensive ETL (Extract, Transform, Load) processing workflow to integrate data from these sources, prepare it for analysis, and load it into a target system.

### **Project Scope:**
#### **Data Extraction:**
The process begins with exporting CSV files from both the (CRM) and (ERP) systems using a pull extraction method with full extraction type using a file parsing extraction technique.

#### **Data Transformation:**
Next, we will perform a full data transformation to ensure data consistency, accuracy, and usability, which includes:
- **Data Cleansing:** Remove duplicates, data filtering, handling missing values, handling invalid values, handling unwanted spaces, and data type casting.
- **Data Normalization:** Structuring the data to reduce redundancy and improve integrity.
- **Data Enrichment:** Adding relevant information to enhance the data's value.

#### **Data Loading:**
We will do a batch processing loading type with full load **(Truncate and Bulk Insert)** loading method, that means we make the table completely empty and then we insert everything from scratch.

#### **Data Warehouse Architecture:**
We will implement a Medallion architecture with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables) for the data warehouse, consisting of three layers:
- **Bronze Layer:** Raw and unstructured data, easy to trace and debug.
- **Silver Layer:** Clean and standardized data (intermediate layer).
- **Gold Layer:** Business-ready data, optimized for analysis and decision-making.

#### **Data Modeling:**
Finally, we will create a star schema for the gold layer, which includes:
- **Fact Table:** The main table containing quantitative data for analysis, tables that contain keys, dates, and measures.
- **Dimension Tables:** Supporting tables containing descriptive attributes related to the facts.

At the end of the project, we will have a **highly refined and aggregated data** product that will be ready for **reporting and analysis**, enabling informed business **decision-making**, the full data architecture appears in this diagram:

![alt text](<Diagrams/Data Warehouse Project Diagram.PNG>)
---

All SQL script for creating the data warehouse are [here](SQL_Scripts), and all diagrams are [here](Diagrams).

## **2. Tools I Used:**
**1. SQL Server Management Studio (SSMS):** RDBMS for managing and interacting with databases.

**2. Visual Studio Code:** for database management and executing SQL queries.

**3. DrawIO:** Design data architecture, models, flows, and diagrams.

**4. Git & GitHub:** for sharing my SQL scripts for creating the data warehouse.

## **3. SQL Scipts:**
### **3.1.Database Creation:**
The first step is to create the database (DataWarehouse), and this SQL script is designed to drop (if it exists) and recreate a database named `DataWarehouse` in a SQL Server instance:
```sql
USE MASTER;
GO

-- Drop the "DataWarehouse" database if exists:
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

-- Recreate Database DataWarehouse:
CREATE DATABASE DataWarehouse;
```
The script first switches to the `master` database. It then checks if the `DataWarehouse` database exists. If it does, the script sets the database to single-user mode, forcing all users to disconnect, and then drops the database. This is useful for cleaning up or recreating a database in a development or testing environment.

Secondly, we would create the three schemas for DataWarehouse data base, bronze, silver and gold schemas:
```sql
-- Create DataWarehouse Database Schemas:
Use DataWarehouse;
GO
-- Create bronze schema:
CREATE SCHEMA bronze;
GO
-- Create silver schema:
CREATE SCHEMA silver;
GO
-- Create gold schema:
CREATE SCHEMA gold;
GO
```

### **3.2.Bronze Layer (Raw Data) Of Medallion Architecture:**
The **Bronze layer** is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "**as-is**". The focus in this layer is quick Change Data Capture and the ability to provide a historical archive of source (cold storage), data lineage, auditability, reprocessing if needed, without rereading the data from the source system.

***3.2.1. Bronze Layer Tables SQL Script:***

So, we would create the structure of the bronze layer tables according to the structure of the main data source, that shown in this SQL script for creating the table structure of the bronze layer from the two source systems (CRM and ERP) :
```sql
USE DataWarehouse;
GO
-- Create table 'bronze.crm_customer_info':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('bronze.crm_customer_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_customer_info;
CREATE TABLE bronze.crm_customer_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(30),
	cst_lastname NVARCHAR(30),
	cst_marital_status NVARCHAR(20),
	cst_gndr NVARCHAR(20),
	cst_create_date DATE
);

-- Create table 'bronze.crm_product_info':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('bronze.crm_product_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_product_info;
CREATE TABLE bronze.crm_product_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(70),
	prd_cost DECIMAL(6,2),
	prd_line NVARCHAR(20),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

-- Create table 'bronze.crm_sales_details':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales DECIMAL(6,2),
	sls_quantity INT,
	sls_price DECIMAL(6,2)
);

-- Create table 'bronze.erp_cust_az12':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(20)
);

-- Create table 'bronze.erp_loc_a101':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

-- Create table 'bronze.erp_px_cat_g1v2':
-- Check if the table exist or not, and if exist we will drop it:
IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(60),
	maintenance NVARCHAR(50)
);
```

***3.2.2. Bronze Layer Tables Full Load (Truncate - Bulk Insert):***

After creating the bronze layer tables structure, we would then perform a full load (truncate - bulk insert) of the data from the two main source systems (ERP & CRM) CSV files to the bronze layer tables.

Before the **bulk insert**, you must do a **truncate** (quickly delete all rows from a table, restarting (restart the auto-increment keys) to an empty state), then bulk insert the data, which is a feature that allows you to efficiently import large volumes of data from a file (text file – csv file) into a SQL Server table.

Common options in bulk insert is the **FIRSTROW** (specifies the starting row in the file), which here **2** as we would load the data from csv files, then **FIELDTERMINATOR** (delimiter between columns), which here is **,** as the data load from csv file and finally **TABLOCK** (locks the table during the bulk insert for better performance).

```sql
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
```
***3.2.3. Bronze Layer Tables Full Load (Truncate - Bulk Insert) Stored Procedure:***

We would create a **Stored Procedure** for a full load (truncate-bulk insert) of the bronze layer, so the code can be reused over and over again (like a function in programming), and then just call it to execute it, which leads to high performance and security. When you call a stored procedure for the first time, SQL Server creates an execution plan and stores it in the cache. In the subsequent executions of the stored procedure, SQL Server reuses the plan to execute the stored procedure very fast with reliable performance.

We would create an **Exception Handling** to the stored procedure, to trace and debug any error apper in the future using **TRY...CATCH** blocks to manage errors gracefully. The `TRY` block contains code that might throw an error, while the `CATCH` block handles the error, allowing for custom error messages or alternative actions.

Also, we would measure the **loading time** for the whole stored procedure, and the loading time for every table full load using date variables with **DATEDIFF** function (calculates the difference between two dates), which takes data part, start date and end date parameters.
```sql
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
```
We can see and trace all bronze layer steps in this diagram that show the steps of the **ETL** of the bronze layer.

![alt text](<Diagrams/Bronze Layer Data Flow Diagram.PNG>)

### **3.3.Silver Layer (Claen and Standaraized Data) Of Medallion Architecture:**
The silver layer is considered the **intermediate layer** of the medallion architecture, which consists of cleaned, filtered, and enriched data. In the silver layer, the data from the bronze layer is matched, merged, conformed, and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts, and transactions, so we would do data transformation and data cleansing such as **remove unwanted spaces (trimming whitespace), data normalization & standardization, handling missing data, remove duplicates, derived columns, data type casting, and data enrichment** to all tables in the bronze layer.  

***3.3.1. Silver Layer Tables SQL Script:***

In this step, we would create the silver layer tables structures depending on the structure of the bronze tables, along with any additional metadata columns that capture the load date/time **(dwh_create_date)**, if we need to edit the structure of the silver layer tables after data transformation to maintain data standardization and casting, we would do that before insert cleaned transformed data into silver layer using **insert into table** (silver tables) depend on select from another table (cleaned bronze tables) by editing the data structure of the silver table by **OBJECT_ID()** function and **If** statement.
```sql
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
```
***3.3.2 Data Transformation (Cleansing) Of Table "silver.crm_customer_info":***
```sql
-- Insert Cleaned Data to "silver.crm_customer_info":
INSERT INTO silver.crm_customer_info (
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
	TRIM(cst_firstname) AS cst_firstname, -- Trimming whitespace
	TRIM(cst_lastname) AS cst_lastname, -- Trimming whitespace
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- Trimming whitespace
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- Trimming whitespace
		ELSE 'N/A' -- Handling missing values
	END cst_marital_status, -- Normalize marital status to readable format
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A' -- Handling missing values
	END cst_gndr, -- Normalize gender to readable format
	cst_create_date
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_ranking
	FROM
		bronze.crm_customer_info
	WHERE 
		cst_id IS NOT NULL
) AS non_duplicate_crm_customer_info 
WHERE
	row_ranking = 1; -- Select the most recent customer by filtering row_ranking
```
***3.3.3 Data Transformation (Cleansing) Of Table "silver.crm_product_info":***
```sql
-- Insert Cleaned Data to "silver.crm_product_info":
-- Before Insert Cleaned Data From Table "bronze.crm_product_info" to Target Table "silver.crm_product_info".
-- We Must Edit On Table Structure to Update Changes:

IF OBJECT_ID('silver.crm_product_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_product_info;
CREATE TABLE silver.crm_product_info(
	prd_id INT,
	cat_id NVARCHAR(50), 
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(70),
	prd_cost DECIMAL(6,2),
	prd_line NVARCHAR(20),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_product_info (
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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category key
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, -- Handling missing values  
	CASE
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line, -- Map product line to make them more descriptive
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS DATE) AS prd_end_dt -- Calculate end date as one date before next start date
FROM
	bronze.crm_product_info;
```

***3.3.4 Data Transformation (Cleansing) Of Table "silver.crm_sales_details":***
```sql
-- Insert Cleaned Data to "silver.crm_sales_details":
-- Before Insert Cleaned Data From Table "bronze.crm_sales_details" to Target Table "silver.crm_sales_details".
-- We Must Edit On Table Structure to Update Changes:

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales DECIMAL(6,2),
	sls_quantity INT,
	sls_price DECIMAL(6,2),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

INSERT INTO silver.crm_sales_details (
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
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Handling invalid data.
        ELSE CAST(CAST(sls_order_dt AS NVARCHAR(20)) AS DATE) -- Data type casting (Data Transformation)
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL -- Handling invalid data.
        ELSE CAST(CAST(sls_ship_dt AS NVARCHAR(20)) AS DATE) -- Data type casting (Data Transformation)
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL -- Handling invalid data.
        ELSE CAST(CAST(sls_due_dt AS NVARCHAR(20)) AS DATE) -- Data type casting (Data Transformation)
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR (sls_sales != ABS(sls_price) * sls_quantity) THEN (ABS(sls_price) * sls_quantity) -- Handling invalid, missing data and business logic.
        ELSE ABS(sls_sales)
    END AS sls_sales,
    sls_quantity AS sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0 THEN (ABS(sls_sales) / NULLIF(sls_quantity, 0)) -- Handling invalid, missing data and business logic.
        ELSE ABS(sls_price)
    END AS sls_price
FROM
    bronze.crm_sales_details;
```
***3.3.5 Data Transformation (Cleansing) Of Table "silver.erp_cust_az12":***

```sql
-- Insert Final Cleaned Data From Table "bronze.erp_cust_az12" To Table "silver.erp_cust_az12":
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Data enrichment to remove unwanted 'NAS' prefix from cid
        ELSE cid
    END AS cid,
    CASE -- Set future birthdates to Null
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE -- Data normalization & standardization
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A' -- Handling missing data
    END AS gen 
FROM
    bronze.erp_cust_az12;
```
***3.3.6 Data Transformation (Cleansing) Of Table "silver.erp_loc_a101":***
```sql
-- Insert Final Cleaned Data From Table "bronze.erp_cust_az12" To Table "silver.erp_cust_az12":
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', '') AS cid, -- Data enrichment to remove unwanted '-' prefix from cid
    CASE
        WHEN UPPER(TRIM(cntry)) IN ('US', 'UNITED STATES', 'USA') THEN 'USA'
        WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
        ELSE TRIM(cntry)
    END AS cntry -- Normalize and handle missing or blank country codes
FROM
    bronze.erp_loc_a101;
```

***3.3.7 Data Transformation (Cleansing) Of Table "silver.erp_px_cat_g1v2":***

```sql
-- Insert Final Cleaned Data From Table "bronze.erp_cust_az12" To Table "silver.erp_cust_az12":
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT 
    id,
    TRIM(cat),
    TRIM(subcat),
    TRIM(maintenance)
FROM
    bronze.erp_px_cat_g1v2;
```

***3.3.8. Silver Layer Tables Full Load (Insert Into Table) Stored Procedure:***

We would create a **Stored Procedure** for a Full Load (Insert Into Table) of the silver cleaned layer, so the code can be reused over and over again (like a function in programming), and then just call it to execute it, which leads to high performance and security. When you call a stored procedure for the first time, SQL Server creates an execution plan and stores it in the cache. In the subsequent executions of the stored procedure, SQL Server reuses the plan to execute the stored procedure very fast with reliable performance.

We would create an **Exception Handling** to the stored procedure, to trace and debug any error apper in the future using **TRY...CATCH** blocks to manage errors gracefully. The `TRY` block contains code that might throw an error, while the `CATCH` block handles the error, allowing for custom error messages or alternative actions.

Also, we would measure the **loading time** for the whole stored procedure, and the loading time for every table full load using date variables with **DATEDIFF** function (calculates the difference between two dates), which takes data part, start date and end date parameters.

```sql
USE DataWarehouse;
GO

-- Create a 'silver.load_silver_sp' strored procedure:
CREATE OR ALTER PROCEDURE silver.load_silver_sp AS 
BEGIN
	DECLARE @procedure_start_time DATETIME,
			@procedure_end_time DATETIME,
			@start_time DATETIME,
			@end_time DATETIME;
	BEGIN TRY
		SET @procedure_start_time = GETDATE()

		PRINT('======================================================');
		PRINT('Loading Silver Layer');
		PRINT('======================================================');

		PRINT('------------------------------------------------------');
		PRINT('Loading CRM System Tables:');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: silver.crm_customer_info');
		TRUNCATE TABLE silver.crm_customer_info;

		PRINT('>> Inserting Table: silver.crm_customer_info');
		INSERT INTO silver.crm_customer_info (
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
			TRIM(cst_firstname) AS cst_firstname, 
			TRIM(cst_lastname) AS cst_lastname, 
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
				ELSE 'N/A'
			END cst_marital_status, 
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'N/A' 
			END cst_gndr, 
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_ranking
			FROM
				bronze.crm_customer_info
			WHERE 
				cst_id IS NOT NULL
		) AS non_duplicate_crm_customer_info 
		WHERE
			row_ranking = 1; 

		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)));
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: silver.crm_product_info');
		TRUNCATE TABLE silver.crm_product_info;

		PRINT('>> Inserting Table: silver.crm_product_info');
		INSERT INTO silver.crm_product_info (
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, 
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost, 
			CASE
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line, 
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS DATE) AS prd_end_dt
		FROM
			bronze.crm_product_info;

		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)));
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: silver.crm_sales_details');
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT('>> Inserting Table: silver.crm_sales_details');
		INSERT INTO silver.crm_sales_details (
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
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR(20)) AS DATE) 
			END AS sls_order_dt,
			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR(20)) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR(20)) AS DATE) 
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR (sls_sales != ABS(sls_price) * sls_quantity) THEN (ABS(sls_price) * sls_quantity) 
				ELSE ABS(sls_sales)
			END AS sls_sales,
			sls_quantity AS sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0 THEN (ABS(sls_sales) / NULLIF(sls_quantity, 0)) 
				ELSE ABS(sls_price)
			END AS sls_price
		FROM
			bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)));

		PRINT('------------------------------------------------------');
		PRINT('Loading ERP System Tables:');
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: silver.erp_cust_az12');
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT('>> Inserting Table: silver.erp_cust_az12');
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
				ELSE cid
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'N/A' 
			END AS gen 
		FROM
			bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)));
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: silver.erp_loc_a101');
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT('>> Inserting Table: silver.erp_loc_a101');
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN UPPER(TRIM(cntry)) IN ('US', 'UNITED STATES', 'USA') THEN 'USA'
				WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END AS cntry 
		FROM
			bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)));
		PRINT('------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncate Table: silver.erp_px_cat_g1v2');
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT('>> Inserting Table: silver.erp_px_cat_g1v2');
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			TRIM(cat),
			TRIM(subcat),
			TRIM(maintenance)
		FROM
			bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT('Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)));

		SET @procedure_end_time = GETDATE();
		PRINT('======================================================');
		PRINT('Loading Silver Layer Is Completed');
		PRINT('Total Load Duration: ' + CAST(DATEDIFF(SECOND, @procedure_start_time, @procedure_end_time) AS NVARCHAR(20)))
		PRINT('======================================================');	
	END TRY
	
	BEGIN CATCH
		PRINT('Error Message: ' + ERROR_MESSAGE());
		PRINT('Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(20)));
		PRINT('Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR(20)));
		PRINT('Error Stored Procedure: ' + ERROR_PROCEDURE());
		PRINT('Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(20)));
		PRINT('Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(20)));
	END CATCH
END;
```

We can see and trace all silver layer steps in this diagram that show the steps of the **TL** of the bronze layer.

![alt text](<Diagrams/Silver Layer Flow Diagram.PNG>)

### **3.4.Gold Layer (Business-Ready Data) Of Medallion Architecture:**

Data in the **Gold layer** of the data warehouse is typically **business-ready data for consumption**. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins. The final layer of data transformations and data quality rules is applied here. Final presentation layer of projects such as Customer Analytics, Product Quality Analytics, Sales Details Analytics, Customer Segmentation, Product Recommendations, Marketing or Sales Analytics.

For creating the gold layer, we need to create a **data modeling** of the silver layer to collect related tables subject into one dimension table and create the fact main table, which contains quantitative data for analysis (keys, measures, and dates).

So, this **integration model** of the silver layer tables shows the related table, which shares the same descriptive attributes, like tables **(crm_customer_info), (erp_cust_az12) and (erp_loc_a101), which share customer-related information**, and tables **(crm_product_info) and (erp_px_cat_g1v2), which share product-related information** and the main fact table **(crm_sales_details)** which contain measures, keys and dates columns.

![alt text](<Diagrams/Data Modeling (Gold Layer).PNG>)

***3.4.1. Create Dimension Customers View***

The dimension of **customers** consists of the **left join between (silver.crm_customer_info) and (silver.erp_cust_az12), then, left join with (silver.erp_loc_a101)**, to create a clear view describing **customer analytics and segmentation**, as view is a virtial table does not have a physical existence on the hard, physically views data base object doesn't store data in the hard, but the data will retrieved to it when call it, to achieve data integrity and consistency.

**Data Integration** Steps Before Create **Gold.Dim_Customers View**:

1. Data integration to **gender columns** in customer tables joins **(cst_gndr - gen)**.
2. Rename columns to **friendly, meaningful names**.
3. Sort the columns into **logical groups to improve readability**.
4. Create a **surrogate key as unique indentifier assigned to each record in a table using ROW_NUMBER() window function**.
5. Mention that this column is a **fact or dimension table**.

```sql
-- Create gold.dim_customers View:
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id ASC) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'N/a' THEN ci.cst_gndr -- CRM system is the master of gender information
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM
    silver.crm_customer_info AS ci LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;
```

***3.4.2. Create Dimension Products View***

The dimension of **products** consists of the **left join between (silver.crm_product_info) and (silver.erp_px_cat_g1v2)**, to create a clear view describing **product quality analytics and product Recommendations**, as view is a virtial table does not have a physical existence on the hard, physically views data base object doesn't store data in the hard, but the data will retrieved to it when call it, to achieve data integrity and consistency.

**Data Integration** Steps Before Create **Gold.Dim_Products View**:

1. Rename columns to **friendly, meaningful names**.
2. Sort the columns into **logical groups to improve readability**.
3. Create a **surrogate key as unique indentifier assigned to each record in a table using ROW_NUMBER() window function**.
4. Mention that this column is a **fact or dimension table**.

```sql
-- Create gold.dim_products View:
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pri.prd_start_dt ASC, pri.prd_key ASC) AS product_key,
    pri.prd_id AS product_id,
    pri.prd_key AS product_number,
    pri.prd_nm AS product_name,
    pri.cat_id AS category_id,
    pca.cat AS category,
    pca.subcat AS sub_category,
    pca.maintenance AS maintenance,
    pri.prd_cost AS cost,
    pri.prd_line AS product_line,
    pri.prd_start_dt AS start_date
FROM
    silver.crm_product_info AS pri LEFT JOIN silver.erp_px_cat_g1v2 AS pca
    ON pri.cat_id = pca.id
WHERE
    pri.prd_end_dt IS NULL; 
```
***3.4.3. Create Fact Sales Details View***

The **fact table** of our data is represented in of **crm_sales_details** table, which consists of left join between **(silver.crm_sales_details)** and **(gold.dim_products)** to get **product_key** (surrogate key) for the products dimension, then, left join with **(gold.dim_customers)** to get **customer_key** (surrogate key) for customers dimension, so our fact tables here achieve fact tables conditions, it contains measures **(sales_amount - quantity - price)**, dates **(order_date - shipping_date - due_date)** and keys **(product_key - customer_key)** columns, finally we would create our fact table in a fact view **(gold.fact_sales_details)**.

**Data Integration** Steps Before Create **Gold.Fact_Sales_Details_View**:

1. Rename columns to **friendly, meaningful names**.
2. Mention that this column is a **fact or dimension table**.

```sql
-- Create gold.fact_sales_details View:
CREATE VIEW gold.fact_sales_details AS 
SELECT
    sd.sls_ord_num AS order_number,
    dp.product_key AS product_key,
    dc.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price 
FROM
    silver.crm_sales_details AS sd LEFT JOIN gold.dim_products AS dp
    ON sd.sls_prd_key = dp.product_number
    LEFT JOIN gold.dim_customers AS dc
    ON sd.sls_cust_id = dc.customer_id;
```
***3.4.4. Create Gold Layer Star Schema Data Modeling***

**Star schema** consists of a **central fact table (gold.fact_sales_details)** connected to **multiple dimension tables (gold.dim_customers) and (gold.dim_products)**, using **One-to-Many** relationship, first one to many relationship is between **(gold.dim_customers) with column (customer_key) and (gold.fact_sales_details) with column (customer_key)**, as many customer can create more than order, second relationship is between **(gold.dim_products) with column (product_key) and (gold.fact_sales_details) with column (product_key)**, as many product can exist in one order, and the star schema modeling is clear shown in this diagram:

![alt text](<Diagrams/DW Star Schema.PNG>)

We can see and trace all gold layer steps in this diagram, alos with the final **Data Flow Diagram Of The Project**:

![alt text](<Diagrams/Gold Layer Flow Diagram.PNG>)

## **4. Conclusion**:

This project successfully established a robust **data warehouse** integrating **CRM** and **ERP** systems, achieving a comprehensive **ETL workflow**. Data extraction via **CSV** files exports, thorough transformation processes including **cleansing, normalization, and enrichment**, and efficient **batch loading ensured data consistency and usability**. The **Medallion architecture**, with its **Bronze, Silver, and Gold layers**, progressively refines data quality. The **star schema** in the Gold layer optimized data for analysis, supporting informed business decision-making. Tools like **SQL Server Management Studio, Visual Studio Code, and DrawIO** facilitated seamless development and collaboration. The final data warehouse product is now ready for reporting and analysis, significantly enhancing the company's data-driven decision-making.
## **5. Data Source**:

This data is a study data from a powerful course (SQL Data Warehouse from Scratch | Full Hands-On Data Engineering Project) from (Data with Baraa) YouTube channel:
https://www.youtube.com/watch?v=9GVqKuTVANE&t=164s 
