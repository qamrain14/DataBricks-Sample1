-- SQL Query: Geographic Sales Analysis
-- =====================================
-- Analyze sales by geographic region

-- Sales by Country
SELECT 
    country,
    COUNT(DISTINCT customer_id) AS customer_count,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(quantity) AS total_items,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    ROUND(AVG(quantity * unit_price), 2) AS avg_order_value,
    ROUND(SUM(quantity * unit_price) / COUNT(DISTINCT customer_id), 2) AS lifetime_value_per_customer
FROM sales
GROUP BY country
ORDER BY total_revenue DESC;


-- Sales by City
SELECT 
    city,
    country,
    COUNT(DISTINCT customer_id) AS customer_count,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    RANK() OVER (PARTITION BY country ORDER BY SUM(quantity * unit_price) DESC) AS city_rank_in_country
FROM sales
GROUP BY city, country
ORDER BY total_revenue DESC;


-- Top Categories by Country
WITH country_category AS (
    SELECT 
        country,
        category,
        ROUND(SUM(quantity * unit_price), 2) AS revenue,
        RANK() OVER (PARTITION BY country ORDER BY SUM(quantity * unit_price) DESC) AS category_rank
    FROM sales
    GROUP BY country, category
)
SELECT * FROM country_category
WHERE category_rank = 1
ORDER BY revenue DESC;
