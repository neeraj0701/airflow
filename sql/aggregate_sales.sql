-- Aggregate sales data to calculate monthly sales metrics
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', sale_date) AS sale_month,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(sales_id) AS total_sales_count,
        SUM(sale_amount) AS total_sales_amount,
        AVG(sale_amount) AS avg_sale_amount
    FROM sales_cleaned
    GROUP BY sale_month
)
INSERT INTO sales_aggregated (sale_month, unique_customers, total_sales_count, total_sales_amount, avg_sale_amount)
SELECT * FROM monthly_sales;

