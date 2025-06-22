-- Cumulative Analysis (RUNNING TOTAL)
-- Calculate the total sales per month
select -- this part to find total sales by month, this is window function
datetrunc(month, order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date)

-- and the running total of sales over time
select
order_date,
total_sales,
sum(total_sales) over(order by order_date ASC) as running_total_sales
From
(
select 
datetrunc(month, order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
-- order by datetrunc(month, order_date) >> remove this because we dont want to sort it by the window function 
) t;

-- running total but reset by year meaning use partition
select
order_date,
total_sales,
sum(total_sales) over(PARTITION BY order_date order by order_date ASC) as running_total_sales
From -- if want a running total by year, removes 'PARTITION BY'
(
select 
datetrunc(month, order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
) t

-- MOVING AVERAGE
select
order_date,
total_sales,
sum(total_sales) over(order by order_date ASC) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_avg_price
From 
(
select 
datetrunc(month, order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
) t