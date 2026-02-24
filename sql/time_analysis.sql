-- SQL Query: Time-Based Sales Analysis
-- =====================================
-- Analyze sales trends over time

-- Daily Sales Summary
SELECT 
    order_date,
    COUNT(DISTINCT order_id) AS orders_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS items_sold,
    ROUND(SUM(quantity * unit_price), 2) AS daily_revenue,
    ROUND(AVG(quantity * unit_price), 2) AS avg_order_value
FROM sales
GROUP BY order_date
ORDER BY order_date;


-- Sales by Day of Week
SELECT 
    DAYOFWEEK(order_date) AS day_of_week,
    CASE DAYOFWEEK(order_date)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name,
    COUNT(DISTINCT order_id) AS orders_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM sales
GROUP BY DAYOFWEEK(order_date)
ORDER BY day_of_week;


-- Monthly Trends (if data spans multiple months)
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    COUNT(DISTINCT order_id) AS orders_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(quantity * unit_price), 2) AS monthly_revenue,
    ROUND(SUM(quantity * unit_price) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer
FROM sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
