--Answering business problems using the Olist Data Warehouse dataset
--Olist Ecommerce: Business Problems List
--1. Customer Cohort Analysis
--Problem: How do customer cohorts behave over time based on their first purchase month?
--	• Objective: Track retention and repeat purchase patterns.
--	• Dataset: orders, customers 
--	• Analysis type: Cohort analysis & retention
--	• Example Insight: “Customers acquired in January 2025 show 40% repeat purchases after 3 months.”
	
	select * ,datediff(month,first_order,order_purchase_timestamp) as rp from(
	select customer_key,order_purchase_timestamp,
	min(order_purchase_timestamp) over(partition by customer_key )as first_order 
	 from gold.fact_orders 
	where customer_key is not null) t
	
	with t as(select customer_key ,count(distinct order_id) as order_count from gold.fact_orders
	where customer_key is not null group by customer_key)
	
	select sum(case when order_count >=2 then 1 else 0 end)*1.0/count(*) as repeat_purchase_rate 
	from t
	
	select *,case when order_count >=2 then 'Y' else 'N' end as Retention from t
	

--2. Customer Lifetime Value (LTV)
--Problem: What is the historical lifetime value of customers and which segments generate the most revenue?
--	• Objective: Identify high-value customers for loyalty programs and marketing prioritization.
--	• Dataset: orders, order_items, customers
--	• Analysis type: LTV calculation
--	• Example Insight: “Top 20% of customers contribute 75% of total revenue.”
	
	with t as (select o.customer_key,sum(oi.price)as LTV from gold.fact_orders o  join gold.fact_order_items oi on o.order_id=oi.order_id
	where order_status='delivered' and customer_key is not null
	group by o.customer_key )
	
	,segmented as (
	select *,ntile(5) over (order by LTV desc) customer_segment from t)
	
	select customer_segment,count(customer_key) as customer_count_in_that_segment
	,sum(LTV) as revenue_per_segment,round(100 * sum(LTV)/(sum(sum(LTV)) over()),2) as pct_revenue from segmented group by customer_segment

--5. Forecasting Orders and Revenue
--Problem: Predict future monthly orders and revenue to optimize inventory and staffing.
--	• Objective: Support operational planning and supply chain management.
--	• Dataset: orders, order_items
--	• Analysis type: Time series forecasting (ARIMA, Prophet, or moving averages)
--	• Example Insight: “Next month’s revenue is forecasted to grow 12% based on seasonal trends.”
--give growth rate
with t as (select  year(order_purchase_timestamp) as year ,month(order_purchase_timestamp)as month,sum(price) as revenue
from gold.fact_orders o join gold.fact_order_items oi on o.order_id=oi.order_id 
where order_status='delivered' group by month(order_purchase_timestamp), year(order_purchase_timestamp) 
)

,growth as (select *,lag(revenue) over(order by year,month ) as previous_month_revenue from t)

select avg(pct_growth_from_previous_month) as growth_rate from (
select *,(revenue-previous_month_revenue)/NULLIF(previous_month_revenue, 0) as pct_growth_from_previous_month from growth
)s


--MOM analysis

with t as (select  year(order_purchase_timestamp) as year ,month(order_purchase_timestamp)as month,sum(price) as revenue
from gold.fact_orders o join gold.fact_order_items oi on o.order_id=oi.order_id 
where order_status='delivered' group by month(order_purchase_timestamp), year(order_purchase_timestamp) 
)

,growth as (select *,lag(revenue) over(order by year,month ) as previous_month_revenue from t)


select *,(revenue-previous_month_revenue)/NULLIF(previous_month_revenue, 0) as pct_growth_from_previous_month from growth



--6. Regional Sales Analysis
--Problem: Which regions/states generate the highest revenue and number of orders?
--	• Objective: Optimize marketing campaigns and logistics in high-performing regions.
--	• Dataset: customers, orders, geolocation
--	• Analysis type: Descriptive, geospatial
--	• Example Insight: “São Paulo and Rio de Janeiro contribute 50% of total revenue.”
with t as (
select c.customer_state,count(distinct o.order_id)as no_of_orders,
sum(oi.price) as revenue from gold.fact_orders o join gold.fact_order_items oi on o.order_id=oi.order_id join
gold.dim_customers c on o.customer_key =c.customer_key
where order_status='delivered' and o.customer_key is not null 
group by c.customer_state  )

select *,100 * revenue/sum(revenue) over () as pct_revenue from t order by pct_revenue desc


--7. Product & Category Performance
--Problem: Identify best-selling products and categories, and products with high returns or low reviews.
--	• Objective: Improve inventory decisions, reduce returns, and enhance product quality.
--	• Dataset: products, order_items, reviews
--	• Analysis type: Descriptive & diagnostic
--	• Example Insight: “Electronics category drives 35% of revenue but has 12% negative reviews.”
with t as (
select p.product_category,count( distinct o.order_id)as order_count, sum(oi.price) as revenue_by_product, COUNT(*) AS units_sold  
from  gold.fact_orders o join gold.fact_order_items oi on o.order_id=oi.order_id 
join gold.dim_products p on oi.product_key=p.product_key 
join gold.fact_reviews r on r.order_id = o.order_id
where o.order_status='delivered' and customer_key is not null 
group by p.product_category )

select product_category,
order_count,revenue_by_product,
 -- % contribution
round(100 * revenue_by_product/sum(revenue_by_product) over(),2) as pct_revenue_product,
 -- cumulative share (Pareto)
ROUND(
    100.0 * SUM(revenue_by_product) OVER (
        ORDER BY revenue_by_product DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / SUM(revenue_by_product) OVER (), 2
) AS cumulative_pct

from t order by revenue_by_product desc



-- Top ~17 categories generate 80% of total revenue
--This is the key business insight.

--?? Business interpretation (this is what matters)
--? Insight 1 — Revenue concentration
--Very few categories drive most revenue:
--~20% categories ? 80% revenue

--Classic Pareto behavior.
--Meaning:
--?? business is dependent on few categories


--8. Delivery Performance Analysis
--Problem: Which sellers or regions experience the longest delivery times?
--	• Objective: Identify logistics bottlenecks and improve delivery efficiency.
--	• Dataset: orders, sellers, geolocation
--	• Analysis type: Diagnostic, geospatial
--	• Example Insight: “Orders to the North region take 20% longer than the national average.”

select s.seller_state,
count(distinct oi.order_id) as total_orders,
sum(o.Flag_delivered_after_estimated)as total_late_deliveries, 
ROUND(100.0 * SUM(o.Flag_delivered_after_estimated) / COUNT(*), 2) AS late_pct
from gold.fact_order_items oi 
join gold.fact_orders o on oi.order_id=o.order_id
join gold.dim_sellers s on oi.seller_key=s.seller_key
where order_status='delivered' 
group by s.seller_state





9. Impact of Delivery on Customer Satisfaction
Problem: Does delivery delay affect customer review scores?
	• Objective: Correlate operational performance with customer satisfaction.
	• Dataset: orders, reviews
	• Analysis type: Correlation analysis
	• Example Insight: “Orders delayed >7 days receive 1.5 points lower on average in reviews.”
WITH review_per_order AS (
    SELECT 
        order_id,
        AVG(score) AS _score
    FROM gold.fact_reviews
    GROUP BY order_id
)

,t as (select o.order_id,o.diff_hours_delivered_to_estimated,
- o.diff_hours_delivered_to_estimated/24 as days_late,
r._score from 
gold.fact_orders o join gold.fact_order_items oi on oi.order_id=o.order_id
join review_per_order r on o.order_id=r.order_id
where order_status='delivered'  )

--select max(days_late),min(days_late) from t
----min is 0 and max days is 188 days
,category as (select 
case when days_late <=0 then 'On time'
when days_late <= 1 then 'minor delay'
when days_late <=3 then 'small'
when days_late <=7 then 'medium'
when days_late <=14 then 'high'
end 
as category_days_late ,_score from t)

select category_days_late ,avg(_score)as review_score from category group by category_days_late order by avg(_score) asc

Negative correlation
“Customer satisfaction declines sharply with delivery delays. Orders delivered on time receive an average rating of 4, while highly delayed deliveries receive only 1. Each delay bucket reduces ratings by ~1 point.”

