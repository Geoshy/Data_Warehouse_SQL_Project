-- Explore Isssues Of Table "bronze.erp_px_cat_g1v2":
USE DataWarehouse
GO;

SELECT * FROM bronze.erp_px_cat_g1v2; -- 37 Low Cardinality Columns

-- Explore Column "id":
-- Column "id" In Table "bronze.erp_px_cat_g1v2" Can Connected With Column "cat_key" In Table "silver.crm_product_info":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2 WHERE id IS NULL; -- 0

--(2) Check Duplicated Values:
SELECT id, COUNT(*) AS id_count
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1; -- 0

-- (3) Check The Availability To Connect Column "id" In Table "bronze.erp_px_cat_g1v2" With Column "cat_key" In Table "silver.crm_product_info":

SELECT cat_id FROM silver.crm_product_info;

SELECT id FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT id 
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT DISTINCT cat_id FROM silver.crm_product_info
); -- 1

-- Explore Column "cat":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2 WHERE cat IS NULL; -- 0

-- (2) Check column normalization & standardization
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;
-- Accessories
-- Bikes
-- Clothing
-- Components

-- (3) Check Unwanted Spaces (Trimming Whitespace): 
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat); -- 0

-- Explore Column "subcat":
-- (1) Check Null Values:
SELECT * FROM bronze.erp_px_cat_g1v2 WHERE subcat IS NULL; -- 0

-- (2) Check column normalization & standardization
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2; -- 37

-- (3) Check Unwanted Spaces (Trimming Whitespace): 
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat); -- 0

-- Explore Column "maintenance":
-- (1) Check Null Values:
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2 WHERE maintenance IS NULL; -- 0

-- (2) Check column normalization & standardization
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2; -- 2

-- (3) Check Unwanted Spaces (Trimming Whitespace): 
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance); -- 0

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

-- Ckeck That The Problems in Bronze Layer Not Occured in Siler Layer:
SELECT * FROM silver.erp_px_cat_g1v2;