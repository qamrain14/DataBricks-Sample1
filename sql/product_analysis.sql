-- SQL Query: Product Performance Analysis
-- =======================================
-- Analyze product and category performance metrics

-- Product Sales Summary
WITH product_metrics AS (
    SELECT 
        product_name,
        category,
        COUNT(*) AS times_ordered,
        SUM(quantity) AS total_quantity_sold,
        SUM(quantity * unit_price) AS total_revenue,
        AVG(unit_price) AS avg_unit_price,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM sales
    GROUP BY product_name, category
)

SELECT 
    product_name,
    category,
    times_ordered,
    total_quantity_sold,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_unit_price, 2) AS avg_unit_price,
    unique_customers,
    ROUND(total_revenue / unique_customers, 2) AS revenue_per_customer,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_rank
FROM product_metrics
ORDER BY total_revenue DESC;


-- Category Performance Summary
SELECT 
    category,
    COUNT(DISTINCT product_name) AS product_count,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_items_sold,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    ROUND(AVG(quantity * unit_price), 2) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM sales
GROUP BY category
ORDER BY total_revenue DESC;
