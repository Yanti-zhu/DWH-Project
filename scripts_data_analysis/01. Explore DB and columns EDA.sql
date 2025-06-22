-- Database Exploration (Explore all objects in the DB)

SELECT * FROM INFORMATION_SCHEMA.TABLES

-- Explore all columns in DB
SELECT * FROM INFORMATION_SCHEMA.columns
WHERE table_name = 'dim_customers'

-- Explore Dimensions, use distinct
select distinct country from gold.dim_customers;
select distinct category, subcategory, product_name from gold.dim_products
order by 1,2,3;

-- Explore Date, check earliest and latest date of the data, use min/ max
select 
	min(order_date) first_order_date,
	max(order_date) last_order_date,
	datediff(year,min(order_date), max(order_date)) AS order_range_months
from gold.fact_sales;

-- find the youngest and oldest customer
select
	min(birthdate) as oldest_birthdate,
	datediff(year, min(birthdate), getdate()) as oldest_age,
	max(birthdate) as youngest_birthdate,
	datediff(year, max(birthdate), getdate()) as youngest_age	
from gold.dim_customers

print '-----------------------------------------------------------'
-- MEASURES EXPLORATION
select * from gold.fact_sales;

-- FIND THE TOTAL SALES
select sum(sales_amount) as total_sales
from gold.fact_sales;

-- FIND HOW MANY ITEMS ARE SOLD
select sum(quantity) as total_quantity from gold.fact_sales

-- FIND THE AVERAGE SELLING PRICE
select avg(price) as avg_price from gold.fact_sales

-- FIND THE TOTAL NUMBER OF ORDERS
select count(order_number) as total_orders from gold.fact_sales
select count(distinct order_number) as total_orders from gold.fact_sales

-- FIND THE TOTAL NUMBER OF PRODUCTS
select count(product_key) as total_products from gold.dim_products
select count(distinct product_key) as total_products from gold.dim_products

-- FIND THE NUMBER OF CUSTOMERS
select count(customer_key) as total_customers from gold.dim_customers

-- FIND THE TOTAL NUMBER OF CUSTOMERS THAT HAS PLACED AN ORDER
select count(distinct customer_key) as total_customers 
from gold.fact_sales

print '--------------------------------------------------'

-- GENERATE A SIMPLE REPORT THAT SHOWS ALL KEY METRICS OF THE BUSINESS
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





