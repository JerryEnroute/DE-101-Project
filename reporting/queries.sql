//Query the top 5 sales by product

SELECT dp.title, SUM(fs.sales) AS total_sales 
FROM fact_sales fs 
JOIN dim_products dp ON fs.product_id = dp.id 
GROUP BY dp.title 
ORDER BY total_sales DESC 
LIMIT 5;

//Query the top 5 sales by category agrupation

SELECT dc.category, SUM(fs.sales) AS total_sales 
FROM fact_sales fs 
JOIN dim_products dp ON fs.product_id = dp.id 
JOIN dim_category dc ON dp.category = dc.category 
GROUP BY dc.category 
ORDER BY total_sales DESC 
LIMIT 5;

//Query the least 5 sales by category agrupation

SELECT dc.category, SUM(fs.sales) AS total_sales 
FROM fact_sales fs 
JOIN dim_products dp ON fs.product_id = dp.id 
JOIN dim_category dc ON dp.category = dc.category  
GROUP BY dc.category 
ORDER BY total_sales ASC 
LIMIT 5;

//Query the top 5 sales by title and subtitle agrupation

SELECT dp.title, dp.subtitle, SUM(fs.sales) AS total_sales 
FROM fact_sales fs 
JOIN dim_products dp ON fs.product_id = dp.id 
GROUP BY dp.title, dp.subtitle 
ORDER BY total_sales DESC 
LIMIT 5;

//Query the top 3 products that has greatest sales by category

WITH RankedSales AS (
    SELECT 
        dp.title, 
        dc.category, 
        SUM(fs.sales) AS total_sales,
        ROW_NUMBER() OVER(PARTITION BY dc.category ORDER BY SUM(fs.sales) DESC) AS rank
    FROM fact_sales fs 
    JOIN dim_products dp ON fs.product_id = dp.id 
    JOIN dim_category dc ON dp.category = dc.category 
    GROUP BY dp.title, dc.category
)

SELECT title, category, total_sales 
FROM RankedSales 
WHERE rank <= 3 
ORDER BY category, total_sales DESC;