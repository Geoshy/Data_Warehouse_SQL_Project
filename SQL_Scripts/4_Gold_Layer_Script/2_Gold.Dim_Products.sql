/*
===========================================================================================================
This SQL script is for create a gold layer dimension table (gold.dim_products) view:

Dimension Products View Consists Of:
    - Left join between (silver.crm_product_info) and (silver.erp_px_cat_g1v2).
===========================================================================================================
*/

USE DataWarehouse;
GO

-- Create Left Join Between Products Tables:
SELECT
    pri.prd_id,
    pri.cat_id,
    pri.prd_key,
    pri.prd_nm,
    pri.prd_cost,
    pri.prd_line,
    pri.prd_start_dt,
    pca.cat,
    pca.subcat,
    pca.maintenance
FROM
    silver.crm_product_info AS pri LEFT JOIN silver.erp_px_cat_g1v2 AS pca
    ON pri.cat_id = pca.id
WHERE
    pri.prd_end_dt IS NULL; -- Filter out all historical data (select only all still producing products) -- 295

-- Make Sure That Join Dose Not Contain Duplicates:
SELECT
    prd_id,
    COUNT(*) AS prd_id_count
FROM (
    SELECT
        pri.prd_id,
        pri.cat_id,
        pri.prd_key,
        pri.prd_nm,
        pri.prd_cost,
        pri.prd_line,
        pri.prd_start_dt,
        pca.cat,
        pca.subcat,
        pca.maintenance
    FROM
        silver.crm_product_info AS pri LEFT JOIN silver.erp_px_cat_g1v2 AS pca
        ON pri.cat_id = pca.id
    WHERE
        pri.prd_end_dt IS NULL
) AS duplicates_check_subquerry
GROUP BY
    prd_id
HAVING
    COUNT(*) > 1; -- 0



/*
Data Integration Steps Before Create Gold.Dim_Products View:
    (1) Rename columns to friendly, meaningful names.
    (2) Sort the columns into logical groups to improve readability.
    (3) Create a surrogate key as unique indentifier assigned to each record in a table using ROW_NUMBER() window function.
    (3) Mention that this column is a fact or dimension table.
*/

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

SELECT * FROM gold.dim_products;