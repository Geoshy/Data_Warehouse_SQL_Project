/*
=======================================================================================================================================
This SQL script is for create a gold layer fact table (gold.fact_sales_details) view:

Fact Sales Details View Consists Of:
    - Left join between (silver.crm_sales_details) and (gold.dim_products) to get product_key (surrogate key) for products dimension.
    - Then, left join with (gold.dim_customers) to get customer_key (surrogate key) for customers dimension.
=======================================================================================================================================
*/

USE DataWarehouse;
GO

SELECT
    sd.sls_ord_num,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price 
FROM
    silver.crm_sales_details AS sd LEFT JOIN gold.dim_products AS dp
    ON sd.sls_prd_key = dp.product_number
    LEFT JOIN gold.dim_customers AS dc
    ON sd.sls_cust_id = dc.customer_id;
/*
Data Integration Steps Before Create Gold.Dim_Products View:
    (1) Rename columns to friendly, meaningful names.
    (2) Mention that this column is a fact or dimension table.
*/

-- Create gold.fact_sales_details View:
CREATE VIEW gold.fact_sales_details AS 
SELECT
    sd.sls_ord_num AS order_number,
    dp.product_key AS product_key,
    dc.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price 
FROM
    silver.crm_sales_details AS sd LEFT JOIN gold.dim_products AS dp
    ON sd.sls_prd_key = dp.product_number
    LEFT JOIN gold.dim_customers AS dc
    ON sd.sls_cust_id = dc.customer_id;

SELECT * FROM gold.fact_sales_details;
SELECT * FROM gold.dim_customers

/*
Fact Check:
    - Check if all dimension tables (gold.dim_customers, gold.dim_products).
    - Can successfully join to fact table (gold.fact_sales_details).
*/

SELECT
    *
FROM
    gold.fact_sales_details AS fs LEFT JOIN gold.dim_products AS dp
    ON fs.product_key = dp.product_key
    WHERE
        dp.product_key IS NULL; -- 0

SELECT
    *
FROM
    gold.fact_sales_details AS fs LEFT JOIN gold.dim_customers AS dc
    ON fs.customer_key = dc.customer_key
WHERE
    dc.customer_key IS NULL; -- 0