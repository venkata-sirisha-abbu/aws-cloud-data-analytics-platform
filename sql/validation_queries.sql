-- Check for null values
SELECT COUNT(*) FROM sales_data WHERE amount IS NULL;

-- Detect negative values
SELECT * FROM sales_data WHERE amount < 0;
