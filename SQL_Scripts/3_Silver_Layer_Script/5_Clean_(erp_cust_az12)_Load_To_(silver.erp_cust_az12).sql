-- Explore Isssues Of Table "bronze.erp_cust_az12":
USE DataWarehouse;
GO

SELECT * FROM bronze.erp_cust_az12; -- 18483

-- Explore Column "cid":
-- Column "cid" In Table "erp_cust_az12" Can Connected With Column "cst_key" In Table "crm_customer_info":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_cust_az12 WHERE cid IS NULL; -- 0

--(2) Check Duplicated Values:
SELECT
    cid,
    COUNT(*)
FROM
    bronze.erp_cust_az12
GROUP BY
    cid
HAVING
    COUNT(*) > 1; -- 0

-- (3) Check The Availability To Connect Column "cid" In Table "erp_cust_az12" With Column "cst_key" In Table "crm_customer_info"
SELECT cid FROM bronze.erp_cust_az12;

SELECT cst_key FROM silver.crm_customer_info;

SELECT * FROM silver.crm_customer_info;

SELECT cid
FROM bronze.erp_cust_az12
WHERE cid NOT IN (
    SELECT cst_key FROM silver.crm_customer_info
); -- 11042
/*
Decision: In "cid" Column The First 3 Characters Not Exist "NAS" In "cst_key" Column.
We Will Extract "cid" Values Without The First 3 Characters.
*/
 
SELECT SUBSTRING(cid, 4, LEN(cid)) AS cid
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%';

SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid
FROM
    bronze.erp_cust_az12
WHERE
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END NOT IN (
        SELECT DISTINCT cst_key FROM silver.crm_customer_info
        ); -- 0

-- Explore Column "bdate":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_cust_az12 WHERE bdate IS NULL; -- 0

-- (2) Check The Validation Of Data Values In Column "bdate"
-- Check Length Of Column, As It Must 10 To Be A Date '1990-03-03':
SELECT LEN(bdate) FROM bronze.erp_cust_az12 WHERE LEN(bdate) != 10; -- 0

-- Check Column Data Tpe:
SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'bronze'
    AND
    TABLE_NAME = 'erp_cust_az12'
    AND
    COLUMN_NAME = 'bdate';
-- bdate -> date

-- Check Date Lower Boundary:
SELECT *
FROM bronze.erp_cust_az12 
WHERE bdate < '1924-01-01'; -- 15 Rows

-- Check Date Upper Boundary:
SELECT *
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE(); -- 16 Rows

SELECT
    *
FROM
    bronze.erp_cust_az12 
WHERE
    bdate < '1924-01-01'
    OR 
    bdate > GETDATE()
ORDER BY
    bdate ASC; -- 31
-- Decision: We Make Sure That Dates That Higher Than GETDATE() Is Not Correct, AS We Will Replace Them With Null.

-- Explore Column "gen":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_cust_az12 WHERE gen IS NULL; -- 1471
SELECT * FROM bronze.erp_cust_az12 WHERE gen IS NULL; 

-- Check Data Normalization & Consistency:
SELECT
    DISTINCT gen
FROM
    bronze.erp_cust_az12;
-- Bad Data: NULL - F - - Male - Female - M
-- Decision: Replace F To Female Values, M To Male Values, And " " To Null Values.

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

-- Ckeck That The Problems in Bronze Layer Not Occured in Siler Layer:
SELECT * FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12 WHERE cid LIKE 'NAS%'; -- 0

SELECT * FROM silver.erp_cust_az12 WHERE bdate > GETDATE(); -- 0

SELECT DISTINCT gen FROM silver.erp_cust_az12;
-- N/A
-- Male
-- Female