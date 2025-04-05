USE DataWarehouse;
GO

-- Explore & Check Issues in Table "bronze.crm_customer_info":
-- Explore Column "cst_id":
-- (1) Check Null Values:

SELECT COUNT(*) FROM bronze.crm_customer_info WHERE cst_id IS NULL; -- 3
SELECT * FROM bronze.crm_customer_info WHERE cst_id IS NULL;
-- Decision: 3 Rows With "cst_id" Null Values Wil Not Selected in "silver.crm_customer_info".  

-- (2) Check For Duplicates Values:

-- First Way (GROUP BY):
SELECT cst_id, COUNT(*) AS cst_id_count
FROM bronze.crm_customer_info
WHERE cst_id IS NOT NULL
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Second Way (WINDOW FUNCTION):
WITH row_ranking_cte AS (
	SELECT
		cst_id,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_id) AS row_ranking
	FROM
		bronze.crm_customer_info
	WHERE
		cst_id IS NOT NULL
)
SELECT * FROM row_ranking_cte WHERE row_ranking > 1;
-- Decision: Order Duplicate Rows Descanding According to Data and The Recent Row Only Selected to "silver.crm_customer_info".

-- Explore Column "cst_key":

-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_customer_info WHERE cst_key IS NULL; -- 0

-- (2) Check For Duplicates Values:
SELECT cst_key, COUNT(*) AS cst_key_count
FROM bronze.crm_customer_info
GROUP BY cst_key
HAVING COUNT(*) > 1; -- 5 Rows

SELECT * FROM bronze.crm_customer_info WHERE cst_key = 'AW00029483';
SELECT * FROM bronze.crm_customer_info WHERE cst_key = 'AW00029466';
SELECT * FROM bronze.crm_customer_info WHERE cst_key = 'AW00029473';
SELECT * FROM bronze.crm_customer_info WHERE cst_key = 'AW00029433';
SELECT * FROM bronze.crm_customer_info WHERE cst_key = 'AW00029449';
-- Decision: Same Duplicates Rows in "cst_key" Column are Same Rows Duplicates in Columns "cst_id".

-- Check for Unwanted Spaces (Trimming Whitespace) in String Columns (cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr):
SELECT cst_key
FROM bronze.crm_customer_info
WHERE cst_key != TRIM(cst_key); -- 0

SELECT cst_firstname
FROM bronze.crm_customer_info
WHERE cst_firstname != TRIM(cst_firstname); -- 15

SELECT cst_lastname
FROM bronze.crm_customer_info
WHERE cst_lastname != TRIM(cst_lastname); -- 17

SELECT cst_marital_status
FROM bronze.crm_customer_info
WHERE cst_marital_status != TRIM(cst_marital_status); -- 0

SELECT cst_gndr
FROM bronze.crm_customer_info
WHERE cst_gndr != TRIM(cst_gndr); -- 0

-- Decision: Trimming Whitespace in ALL String Columns Befire Load to "silver.crm_customer_info".

-- Check Data Standarization and Consistency:
SELECT DISTINCT cst_marital_status FROM bronze.crm_customer_info;
-- Decision: S >> Single - M >> Married - Null >> N/A.

SELECT DISTINCT cst_gndr FROM bronze.crm_customer_info;
-- Decision: F >> Female - M >> Male - Null >> N/A.

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

-- Ckeck That The Problems in Bronze Layer Not Occured in Siler Layer:

SELECT cst_id, COUNT(*) AS cst_id_count
FROM silver.crm_customer_info
WHERE cst_id IS NOT NULL
GROUP BY cst_id
HAVING COUNT(*) > 1; -- 0

SELECT cst_firstname
FROM silver.crm_customer_info
WHERE cst_firstname != TRIM(cst_firstname); -- 0

SELECT cst_lastname
FROM silver.crm_customer_info
WHERE cst_lastname != TRIM(cst_lastname); -- 0

SELECT DISTINCT cst_marital_status FROM silver.crm_customer_info;
-- Single - Married

SELECT DISTINCT cst_gndr FROM silver.crm_customer_info;
-- Male - Female - N/A
