USE DataWarehouse;
GO

SELECT * FROM bronze.crm_product_info; -- 397

-- Explore & Check Issues in Table "bronze.crm_product_info":
-- Explore Column "prd_id":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_product_info WHERE prd_id IS NULL; -- 0

-- (2) Check For Duplicates Values:
SELECT prd_id, COUNT(*) AS prd_id_count
FROM bronze.crm_product_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; -- 0

-- Explore Column "prd_key":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_product_info WHERE prd_key IS NULL; -- 0

-- (2) Check for Unwanted Spaces (Trimming Whitespace):
SELECT prd_key
FROM bronze.crm_product_info
WHERE prd_key != TRIM(prd_key); -- 0

-- (3) We Find That First 5 Characters are Category Id & Remain Characters are the Acutal Product Key:
SELECT REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') FROM bronze.crm_product_info; -- CO-RF
SELECT id FROM bronze.erp_px_cat_g1v2; -- AC_BR

SELECT REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')
FROM bronze.crm_product_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') IN (SELECT id FROM bronze.erp_px_cat_g1v2); -- 390

SELECT SUBSTRING(prd_key, 7, LEN(prd_key)) FROM bronze.crm_product_info; -- FR-R92B-58
SELECT sls_prd_key FROM bronze.crm_sales_details; -- BK-M82S-44

SELECT SUBSTRING(prd_key, 7, LEN(prd_key)) 
FROM bronze.crm_product_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (SELECT sls_prd_key FROM bronze.crm_sales_details); -- 177

-- Explore Column "prd_nm":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_product_info WHERE prd_nm IS NULL; -- 0

-- (2) Check for Unwanted Spaces (Trimming Whitespace):
SELECT prd_nm
FROM bronze.crm_product_info
WHERE prd_nm != TRIM(prd_nm); -- 0

-- (3) Check On Standardization Of The Values:
SELECT DISTINCT prd_nm FROM bronze.crm_product_info; -- 295

-- Explore Column "prd_cost":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_product_info WHERE prd_cost IS NULL; -- 2
SELECT * FROM bronze.crm_product_info WHERE prd_cost IS NULL; -- 2
-- Decision: Replace Null Values in prd_cost With 0 to Avoid Affect Average Value.
SELECT ISNULL(prd_cost, 0) FROM bronze.crm_product_info; -- 0.00

-- Explore Column "prd_line":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.crm_product_info WHERE prd_line IS NULL; -- 17
SELECT * FROM bronze.crm_product_info WHERE prd_line IS NULL; -- 17

-- (2) Check On Standardization Of The Values:
SELECT DISTINCT prd_line FROM bronze.crm_product_info; -- Null, M, R, S, T

-- Explore "prd_start_dt" & "prd_end_dt":
SELECT *
FROM bronze.crm_product_info
WHERE
	prd_start_dt > prd_end_dt
	OR
	prd_end_dt IS NULL; -- 397
-- Decision: We Have a 200 Row That Hve a Problem Of (prd_start_dt > prd_end_dt) and Remains Have Null prd_end_dt Values.

-- We Will Replace prd_end_dt With Next (prd_start_dt - 1), That More Logic & Correct, Then Convert Them To DATE Data Type:
SELECT
	prd_id,
	prd_start_dt,
	prd_end_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(ORDER BY prd_id ASC)) AS edited_prd_end_date
FROM bronze.crm_product_info
WHERE prd_start_dt > prd_end_dt;

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS edited_prd_end_date,
	prd_end_dt
FROM bronze.crm_product_info
WHERE prd_key IN ('AC-HE-HL-U509', 'AC-HE-HL-U509-R');

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

-- Ckeck That The Problems in Bronze Layer Not Occured in Siler Layer:
SELECT * FROM silver.crm_product_info;

SELECT * FROM silver.crm_product_info WHERE prd_cost IS NULL; -- 0 Rows

SELECT DISTINCT prd_line FROM silver.crm_product_info;
-- Mountain
-- N/A
-- Other Sales
-- Road
-- Touring

SELECT *
FROM silver.crm_product_info
WHERE prd_start_dt > prd_end_dt; -- 0 Rows