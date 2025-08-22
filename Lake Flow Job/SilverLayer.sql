
CREATE OR REPLACE TABLE sales.silver.netflix_cleaned USING DELTA AS
SELECT
    Show_id,
    Category,
    Title,
    Country,
    Rating,
    Duration,
    Type,
    Description
FROM sales.bronze.netflix_data WHERE country IS NOT NULL
  AND show_id IS NOT NULL;
  
