-- Gold Layer: Aggregated insights
CREATE OR REPLACE TABLE sales.gold.netflix_summary  AS
SELECT
    category,                      
    type,                          
    COUNT(*) AS total_titles,
    COUNT(DISTINCT country) AS countries_available,
    COUNT(DISTINCT rating) AS unique_ratings
FROM sales.silver.netflix_cleaned
GROUP BY category, type
ORDER BY total_titles DESC;

SELECT * FROM sales.gold.netflix_summary
