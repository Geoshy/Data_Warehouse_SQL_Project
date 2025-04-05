/*
===========================================================================================================
This SQL script is for create a gold layer dimension table (gold.dim_customers) view:

Dimension Customers View Consists Of:
    - Left join between (silver.crm_customer_info) and (silver.erp_cust_az12).
    - Then, left join with (silver.erp_loc_a101 )
===========================================================================================================
*/

USE DataWarehouse;
GO

-- Create Left Join Between Customers Tables:
SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM
    silver.crm_customer_info AS ci LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

-- Make Sure That Join Dose Not Contain Duplicates:
SELECT 
    cst_id,
    COUNT(*) cst_id_count
FROM (
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM
        silver.crm_customer_info AS ci LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 AS la
        ON ci.cst_key = la.cid
) AS duplicated_check_subquery
GROUP BY 
    cst_id
HAVING
    COUNT(*) < 1; -- 0

-- Data Integration Of Gender Columns:    
SELECT
    DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- CRM system is the master of gender information
        ELSE COALESCE(ca.gen, 'N/A')
    END AS new_gen
FROM
    silver.crm_customer_info AS ci LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid
ORDER BY
    1,
    2;

/*
Data Integration Steps Before Create Gold.Dim_Customers View:
    (1) Data integration to gender columns in customer tables joins (cst_gndr - gen).
    (2) Rename columns to friendly, meaningful names.
    (3) Sort the columns into logical groups to improve readability.
    (4) Create a surrogate key as unique indentifier assigned to each record in a table using ROW_NUMBER() window function.
    (5) Mention that this column is a fact or dimension table.
*/

SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id ASC) AS customer_key,
    ci.cst_id AS customer_name,
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

SELECT * FROM gold.dim_customers;