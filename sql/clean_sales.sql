-- Remove duplicates and handle missing values in sales_raw
WITH cleaned_sales AS (
    SELECT
        sales_id,
        customer_id,
        product_id,
        sale_date,
        sale_amount
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY sales_id ORDER BY sale_date DESC) AS row_num
        FROM sales_raw
    ) AS temp
    WHERE row_num = 1  -- Keep the latest record in case of duplicates
    AND sale_amount IS NOT NULL  -- Remove rows with null sale amounts
)
INSERT INTO sales_cleaned (sales_id, customer_id, product_id, sale_date, sale_amount)
SELECT * FROM cleaned_sales;


