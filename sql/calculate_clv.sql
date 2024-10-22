-- Calculate Customer Lifetime Value (CLV)
WITH total_sales_per_customer AS (
    SELECT
        customer_id,
        SUM(sale_amount) AS total_spent,
        COUNT(sales_id) AS total_orders
    FROM sales_cleaned
    GROUP BY customer_id
),
avg_sale_per_customer AS (
    SELECT
        customer_id,
        AVG(sale_amount) AS avg_order_value
    FROM sales_cleaned
    GROUP BY customer_id
)
INSERT INTO customer_lifetime_value (customer_id, total_spent, total_orders, avg_order_value)
SELECT
    t.customer_id,
    t.total_spent,
    t.total_orders,
    a.avg_order_value
FROM total_sales_per_customer t
JOIN avg_sale_per_customer a ON t.customer_id = a.customer_id;

