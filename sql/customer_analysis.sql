-- SQL Query: Customer Sales Analysis
-- ===================================
-- This query analyzes customer purchasing behavior
-- Can be executed in Databricks SQL or via PySpark SQL

-- Create temporary view (run this first in PySpark)
-- spark.sql("CREATE OR REPLACE TEMP VIEW sales AS SELECT * FROM ...")

-- Customer Summary with Rankings
WITH customer_orders AS (
    SELECT 
        customer_id,
        first_name || ' ' || last_name AS full_name,
        email,
        city,
        country,
        order_id,
        product_name,
        category,
        quantity,
        unit_price,
        quantity * unit_price AS total_amount,
        order_date
    FROM sales
),

customer_summary AS (
    SELECT 
        customer_id,
        full_name,
        email,
        city,
        country,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_spent,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        SUM(quantity) AS total_items_bought
    FROM customer_orders
    GROUP BY 
        customer_id, full_name, email, city, country
)

SELECT 
    customer_id,
    full_name,
    email,
    city,
    country,
    CASE 
        WHEN total_spent >= 1000 THEN 'Gold'
        WHEN total_spent >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier,
    RANK() OVER (ORDER BY total_spent DESC) AS spending_rank,
    total_orders,
    total_items_bought,
    ROUND(total_spent, 2) AS total_spent,
    ROUND(avg_order_value, 2) AS avg_order_value,
    first_order_date,
    last_order_date
FROM customer_summary
ORDER BY spending_rank;
