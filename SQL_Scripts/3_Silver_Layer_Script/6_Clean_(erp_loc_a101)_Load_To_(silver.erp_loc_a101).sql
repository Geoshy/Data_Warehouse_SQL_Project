-- Explore Isssues Of Table "bronze.erp_loc_a101":
USE DataWarehouse
GO;

SELECT * FROM bronze.erp_loc_a101; -- 18484

-- Explore Column "cid":
-- Column "cid" In Table "erp_loc_a101" Can Connected With Column "cst_key" In Table "crm_customer_info":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_loc_a101 WHERE cid IS NULL; -- 0

--(2) Check Duplicated Values:
SELECT
    cid,
    COUNT(*) cid_count
FROM
    bronze.erp_loc_a101
GROUP BY
    cid
HAVING
    COUNT(*) > 1; -- 0 

-- (3) Check The Availability To Connect Column "cid" In Table "erp_loc_a101" With Column "cst_key" In Table "crm_customer_info"
SELECT cst_key
FROM silver.crm_customer_info;

SELECT cid
FROM bronze.erp_loc_a101;

SELECT *
FROM bronze.erp_loc_a101
WHERE cid NOT IN (
    SELECT cst_key FROM silver.crm_customer_info
); -- 18282

SELECT *
FROM bronze.erp_loc_a101
WHERE LEN(cid) != 11; -- 0

SELECT REPLACE(cid, '-', '')
FROM bronze.erp_loc_a101;

/*
Decision: We Wiil Replace '-' With '' In 'cid' Column In Table 'erp_loc_a101' To Join It With Column 'cst_key' In 'crm_customer_info' Table.
*/

SELECT *
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
    SELECT cst_key FROM silver.crm_customer_info
); -- 0


-- Explore Column "cntry":
-- (1) Check Null Values:
SELECT * FROM bronze.erp_loc_a101 WHERE cntry IS NULL; -- 332

-- (2) Check column normalization & standardization
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;
-- DE
-- USA
-- Germany
-- United States
-- NULL
-- Australia
-- United Kingdom
--  
-- Canada
-- France
-- US

/*
Decision: 
- Replace (USA - US - United States) With (USA).
- Replace (Null - 'empty') With N/A.
- Replace (DE) With Germany (DE Country Code To Germany).
*/

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

-- Ckeck That The Problems in Bronze Layer Not Occured in Siler Layer:
SELECT * FROM silver.erp_loc_a101;

SELECT cid FROM silver.erp_loc_a101;

SELECT DISTINCT cntry
FROM silver.erp_loc_a101;