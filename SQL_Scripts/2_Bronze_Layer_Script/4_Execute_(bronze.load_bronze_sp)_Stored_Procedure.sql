/*
======================================================================================================
This SQL Script is to Execute the Stored Procedure "bronze.load_bronze_sp":
======================================================================================================
*/

USE DataWarehouse;
GO

EXEC bronze.load_bronze_sp; 

/*
======================================================
Loading Bronze Layer
======================================================
------------------------------------------------------
Loading CRM System Tables:
------------------------------------------------------
>> Truncate Table: bronze.crm_customer_info
>> Inserting Table: bronze.crm_customer_info

(18493 rows affected)
Load Duration: 0 Seconds
------------------------------------------------------
>> Truncate Table: bronze.crm_product_info
>> Inserting Table: bronze.crm_product_info

(397 rows affected)
Load Duration: 0 Seconds
------------------------------------------------------
>> Truncate Table: bronze.crm_sales_details
>> Inserting Table: bronze.crm_sales_details

(60398 rows affected)
Load Duration: 0 Seconds
------------------------------------------------------
Loading ERP System Tables:
------------------------------------------------------
>> Truncate Table: bronze.erp_cust_az12
>> Inserting Table: bronze.erp_cust_az12

(18483 rows affected)
Load Duration: 0 Seconds
------------------------------------------------------
>> Truncate Table: bronze.erp_loc_a101
>> Inserting Table: bronze.erp_loc_a101

(18484 rows affected)
Load Duration: 0 Seconds
------------------------------------------------------
>> Truncate Table: bronze.erp_px_cat_g1v2
>> Inserting Table: bronze.erp_px_cat_g1v2

(37 rows affected)
Load Duration: 0 Seconds
------------------------------------------------------
======================================================
Loading Bronze Layer is Completed
Total Load Duration: 0 Seconds
======================================================

Completion time: 2025-03-09T10:08:24.6242826+02:00
*/