-- Explore & Check Issues in Table "bronze.crm_product_info":
-- Explore Column "prd_id":
-- (1) Check Null Values:
-- (2) Check For Duplicates Values:
-- (3) Check On Standardization Of The Values:
-- Decision:

USE DataWarehouse;
GO

SELECT * FROM bronze.crm_sales_details;

-- Explore Column "sls_ord_num":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_ord_num IS NULL; -- 0

-- (2) Check For Duplicates Values:
SELECT
    sls_ord_num,
    COUNT(*)
FROM
    bronze.crm_sales_details
GROUP BY
    sls_ord_num
HAVING
     COUNT(*) > 1;

SELECT * FROM bronze.crm_sales_details WHERE sls_ord_num = 'SO62535';
-- Decision: Duplicated Values are Fine as We Can Have Different sls_prd_key to Every sls_ord_num.

-- (3) Check Of Unwanted Spaces (Trimming Whitespace):
SELECT * FROM bronze.crm_sales_details WHERE sls_ord_num != TRIM(sls_ord_num); -- 0

-- Explore Column "sls_prd_key":
-- -- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_prd_key IS NULL; -- 0

-- (2) Check Of Unwanted Spaces (Trimming Whitespace):
SELECT * FROM bronze.crm_sales_details WHERE sls_prd_key != TRIM(sls_prd_key); -- 0

-- (3) Check the Integrity of Column "" as a Foreign Key in "" With Column "" in "" as Primary Key:
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key
    FROM silver.crm_product_info
); -- 0

-- Explore Column "sls_cust_id":
-- -- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_cust_id IS NULL; -- 0

-- (2) Check the Integrity of Column "" as a Foreign Key in "" With Column "" in "" as Primary Key:
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
    FROM silver.crm_customer_info
); -- 0

-- (3) Check the Zero or Negative Values:
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id <= 0; -- 0

-- Explore Column "sls_order_dt":
-- -- (1) Check Null Values:
SELECT * FROM bronze.crm_sales_details WHERE sls_order_dt IS NULL; -- 0

-- Ckeck Quality Of Data in Column "sls_order_dt"
-- (2) Check the Zero or Negative Values:
SELECT * FROM bronze.crm_sales_details WHERE sls_order_dt = 0; -- 17 Rows

SELECT NULLIF(sls_order_dt, 0) 
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0 ; -- 17 NULL Rows
-- Decision: Replace a Zero Values With Nulls to Avoid Error When Converting Column to Date Data Type.

SELECT * FROM bronze.crm_sales_details WHERE sls_order_dt < 0; -- 0

-- (3) Check for Outliers by Validating the Boundaries of the Date Range: 
SELECT
    CAST(CAST(sls_order_dt AS NVARCHAR(50)) AS DATE) AS sls_order_dt
FROM
    bronze.crm_sales_details
WHERE
    CAST(CAST(sls_order_dt AS NVARCHAR(50)) AS DATE) > GETDATE()
    OR
    sls_order_dt < 19900901; -- 0

-- (4) Check the Upper & Lower Boundary of Column Length:
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE 
    (LEN(sls_order_dt) > 8
    OR
    LEN(sls_order_dt) < 8)
    AND
    sls_order_dt != 0; -- 2 Rows
-- Decision: sls_order_dt That Less That (8) Will Not Selected.

-- Explore Column "sls_ship_dt":
-- -- (1) Check Null Values:
SELECT * FROM bronze.crm_sales_details WHERE sls_ship_dt IS NULL; -- 0

-- Ckeck Quality Of Data in Column "sls_ship_dt"
-- (2) Check the Zero or Negative Values:
SELECT * FROM bronze.crm_sales_details WHERE sls_ship_dt = 0; -- 0 

SELECT * FROM bronze.crm_sales_details WHERE sls_ship_dt < 0; -- 0

-- (3) Check for Outliers by Validating the Boundaries of the Date Range: 
SELECT
    CAST(CAST(sls_ship_dt AS NVARCHAR(50)) AS DATE) AS sls_ship_dt
FROM
    bronze.crm_sales_details
WHERE
    CAST(CAST(sls_ship_dt AS NVARCHAR(50)) AS DATE) > GETDATE()
    OR
    sls_ship_dt < 19900901; -- 0

-- (4) Check the Upper & Lower Boundary of Column Length:
SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE 
    (LEN(sls_ship_dt) > 8
    OR
    LEN(sls_ship_dt) < 8)
    AND
    sls_ship_dt != 0; -- 0

-- Explore Column "sls_due_dt":
-- -- (1) Check Null Values:
SELECT * FROM bronze.crm_sales_details WHERE sls_due_dt IS NULL; -- 0

-- Ckeck Quality Of Data in Column "sls_due_dt"
-- (2) Check the Zero or Negative Values:
SELECT * FROM bronze.crm_sales_details WHERE sls_due_dt = 0; -- 0 

SELECT * FROM bronze.crm_sales_details WHERE sls_due_dt < 0; -- 0

-- (3) Check for Outliers by Validating the Boundaries of the Date Range: 
SELECT
    CAST(CAST(sls_due_dt AS NVARCHAR(50)) AS DATE) AS sls_due_dt
FROM
    bronze.crm_sales_details
WHERE
    CAST(CAST(sls_due_dt AS NVARCHAR(50)) AS DATE) > GETDATE()
    OR
    sls_due_dt < 19900901; -- 0

-- (4) Check the Upper & Lower Boundary of Column Length:
SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE 
    (LEN(sls_due_dt) > 8
    OR
    LEN(sls_due_dt) < 8)
    AND
    sls_due_dt != 0; -- 0

-- Check for Invalid Data Orders:
SELECT
    CAST(CAST(sls_order_dt AS NVARCHAR(20)) AS DATE) AS sls_order_dt,
    CAST(CAST(sls_ship_dt AS NVARCHAR(20)) AS DATE) AS sls_ship_dt,
    CAST(CAST(sls_due_dt AS NVARCHAR(20)) AS DATE) AS sls_due_dt
FROM
    bronze.crm_sales_details
WHERE
    (sls_order_dt != 0 AND LEN(sls_order_dt) = 8)
    AND
    (sls_ship_dt != 0 AND LEN(sls_ship_dt) = 8)
    AND
    (sls_due_dt != 0 AND LEN(sls_due_dt) = 8)
    AND
    (CAST(CAST(sls_order_dt AS NVARCHAR(20)) AS DATE) > CAST(CAST(sls_ship_dt AS NVARCHAR(20)) AS DATE))
    OR
    (CAST(CAST(sls_ship_dt AS NVARCHAR(20)) AS DATE) > CAST(CAST(sls_due_dt AS NVARCHAR(20)) AS DATE)); -- 0

-- Explore Column "sls_sales":
-- -- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_sales IS NULL; -- 8 Rows
SELECT * FROM bronze.crm_sales_details WHERE sls_sales IS NULL;

-- (2) Check the Zero or Negative Values:
SELECT sls_sales FROM bronze.crm_sales_details WHERE sls_sales = 0; -- 2 Rows
SELECT sls_sales FROM bronze.crm_sales_details WHERE sls_sales < 0; -- 3 Rows

-- Explore Column "sls_quantity":
-- -- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_quantity IS NULL; -- 0 Rows

-- (2) Check the Zero or Negative Values:
SELECT sls_sales FROM bronze.crm_sales_details WHERE sls_quantity = 0; -- 0 Rows
SELECT sls_sales FROM bronze.crm_sales_details WHERE sls_quantity < 0; -- 0 Rows

-- Explore Column "sls_price":
-- -- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_price IS NULL; -- 7 Rows
SELECT * FROM bronze.crm_sales_details WHERE sls_price IS NULL;
-- Decision: Calculate sls_sales As It Equal (sls_price * sls_quantity)

-- (2) Check sls_price Values When sls_quantity > 1:
SELECT * FROM bronze.crm_sales_details WHERE sls_quantity > 1 AND sls_price IS NOT NULL; -- 6 Rows
-- Decision: Calculate sls_sales As It Equal (sls_price * sls_quantity) AS They Not Correct In Not Null Values.

SELECT
    sls_sales,
    sls_quantity,
    CASE
        WHEN sls_quantity > 1 OR sls_price IS NULL THEN (sls_sales * sls_quantity)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE
    sls_quantity > 1
    OR
    sls_price IS NULL; -- 13 Rows

-- (2) Check the Zero or Negative Values:
SELECT sls_price FROM bronze.crm_sales_details WHERE sls_price = 0; -- 0 Rows
SELECT sls_price FROM bronze.crm_sales_details WHERE sls_price < 0; -- 5 Rows

-- Check for Invalid Business Rules Calculation:
SELECT
    DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM
    bronze.crm_sales_details
WHERE
    (sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL)
    OR
    (sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0)
    OR
    (sls_sales != sls_price * sls_quantity)
ORDER BY
    sls_sales ASC,
    sls_quantity ASC,
    sls_price ASC; -- 33  Rows

-- Rules For Fixing The Issues Of (Null Values - Zero Values - Negative Values - Non Business Logic Values):
-- If Sales is Negative, Zero, or Null derive it Using Quantity and Price.
-- If Price is Null or Zero Calculate it Using Sales and Quantity.
-- If Price is Negative Convert It to Positive.

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

-- Ckeck That The Problems in Bronze Layer Not Occured in Siler Layer:
SELECT * FROM silver.crm_sales_details;

SELECT * 
FROM silver.crm_sales_details
WHERE 
    (sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL)
    OR
    (sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0)
    OR
    (sls_sales != sls_quantity * sls_price); -- 0 Rows
