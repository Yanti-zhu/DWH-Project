-- 01. BASIC REPORT THAT SHOWS ALL KEY METRICS OF THE BUSINESS

select 'Total Sales' as measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
select 'Total Quantity' as measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
select 'Average Price', avg(price) from gold.fact_sales
UNION ALL
select 'Total Nr. Orders', count(distinct order_number) from gold.fact_sales
UNION ALL
select 'Total Nr. Products', count(product_name) from gold.dim_products
UNION ALL
select 'Total Nr. Customers', count(customer_key) from gold.dim_customers;