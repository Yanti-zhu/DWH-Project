/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics: >> 2nd CTE for Aggregation - THIS IS 2ND STEP
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/


IF OBJECT_ID('gold.vw_report_customers', 'V') IS NOT NULL
    DROP VIEW gold.vw_report_customers;
GO

CREATE VIEW gold.vw_report_customers AS

/* ----------------------------------------------------------------------------
1. Base Query: Retrieves core columns from tables
-------------------------------------------------------------------------------*/

WITH cte_base_query AS(
Select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name, ' ' , c.last_name) as customer_name,
datediff(year, birthdate, GETDATE()) as age
from 
gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where order_date is not null
)

/*---------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
---------------------------------------------------------------------------*/
, cte_customer_aggregation AS ( -- THIS IS THE START OF 2ND CTE FOR AGGREGATION
SELECT -- This is used to buil 2nd CTE, Aggregation to answer qusetion 3 above
customer_key,
customer_number,
customer_name,
age,
count(DISTINCT order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(DISTINCT product_key) as total_products,
max(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as lifespan
from cte_base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age
) -- END OF 2ND CTE

SELECT -- THIS STATEMENT ANSWERS QUESTION 2
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age < 20 THEN 'Under 20'
	 WHEN age between 20 and 29 THEN '20-29'
	 WHEN age between 30 and 39 THEN '30-39'
	 WHEN age between 40 and 49 THEN '40-49'
	 ELSE '50+'
END AS age_group,
CASE 
    WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
    WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
    ELSE 'New'
END AS customer_segment,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
-- Compute Avg order value
CASE WHEN total_orders = 0 THEN 0
	ELSE total_sales / total_orders 
END AS avg_order_value,
-- Compute avg monthly spend
CASE WHEN lifespan = 0 THEN total_sales
	ELSE total_sales / lifespan 
END AS avg_monthly_spend
from cte_customer_aggregation

-- END of the statement, NEXT put this in as a VIEW to use for users go to the very top and add CREATE VIEW...