/*
======================================================================================================
This SQL Script is to Execute the Stored Procedure "silver.load_silver_sp":
======================================================================================================
*/

USE DataWarehouse;
GO

-- ETL Process (Truncate Then Load Bronze Layer):
EXEC bronze.load_bronze_sp;

-- ETL Process (Truncate Then Load Silver Layer):
EXEC silver.load_silver_sp; 

/*

Timestamp	Message
[9:50:08 AM]	Started executing query at Line 10
======================================================
Loading Silver Layer
======================================================
------------------------------------------------------
Loading CRM System Tables:
------------------------------------------------------
>> Truncate Table: silver.crm_customer_info
>> Inserting Table: silver.crm_customer_info
(18484 rows affected)
Load Duration: 1
------------------------------------------------------
>> Truncate Table: silver.crm_product_info
>> Inserting Table: silver.crm_product_info
(397 rows affected)
Load Duration: 0
------------------------------------------------------
>> Truncate Table: silver.crm_sales_details
>> Inserting Table: silver.crm_sales_details
(60398 rows affected)
Load Duration: 0
------------------------------------------------------
Loading ERP System Tables:
------------------------------------------------------
>> Truncate Table: silver.erp_cust_az12
>> Inserting Table: silver.erp_cust_az12
(18483 rows affected)
Load Duration: 1
------------------------------------------------------
>> Truncate Table: silver.erp_loc_a101
>> Inserting Table: silver.erp_loc_a101
(18484 rows affected)
Load Duration: 0
------------------------------------------------------
>> Truncate Table: silver.erp_px_cat_g1v2
>> Inserting Table: silver.erp_px_cat_g1v2
(37 rows affected)
Load Duration: 0
======================================================
Loading Silver Layer Is Completed
Total Load Duration: 2
======================================================
Total execution time: 00:00:01.289
*/

