-- Performance analysis eg, YoY, actual vs target

/*ANALYSE THE YEARLY PERFORMANCE OF PRODUCTS BY COMPARING THEIR SALES TO BOTH THE AVG SALES PERFORMANCE
AND THE PREVIOUS YEAR'S SALES */

--This is the basic calc, to find the comparison, we can use CTE
use DataWarehouse	
SELECT 
Year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
FROM gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by
year(f.order_date),
p.product_name

-- add CTE

With yearly_product_sales AS (
SELECT 
Year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
FROM gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by
year(f.order_date),
p.product_name
)

select 
order_year, 
product_name,
current_sales,
avg(current_sales) over (partition by product_name) avg_sales,
current_sales - avg(current_sales) over (partition by product_name) AS diff_avg,
CASE WHEN current_sales - avg(current_sales) over (partition by product_name) > 0 THEN 'Above Avg'
	WHEN current_sales - avg(current_sales) over (partition by product_name) < 0 THEN 'Below Avg'
	ELSE 'Avg'
END avg_change,
LAG(current_sales) over (Partition by product_name order by order_year ASC) py_sales,
current_sales - LAG(current_sales) over (Partition by product_name order by order_year ASC) as yoy_diff,
CASE WHEN current_sales - LAG(current_sales) over (Partition by product_name order by order_year ASC) > 0 THEN 'Increase'
	WHEN current_sales - LAG(current_sales) over (Partition by product_name order by order_year ASC) < 0 THEN 'Decrease'
	ELSE 'No change'
END yoy_change
from yearly_product_sales
order by product_name, order_year