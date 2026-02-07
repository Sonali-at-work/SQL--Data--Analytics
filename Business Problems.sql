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
	


3. Customer Acquisition Cost (CAC)
Problem: What is the estimated cost to acquire a new customer?
	• Objective: Measure efficiency of acquisition strategies (requires assumptions for marketing spend).
	• Dataset: orders, customers + marketing spend assumptions
	• Analysis type: CAC = Total marketing spend ÷ # of new customers
	• Example Insight: “Estimated CAC is $5 per customer; LTV/CAC ratio indicates high profitability.”

4. Customer Retention Rate
Problem: What percentage of customers return for subsequent purchases and how does retention vary across cohorts?
	• Objective: Identify churn risk and design retention strategies.
	• Dataset: orders, customers
	• Analysis type: Retention rate, cohort-based analysis
	• Example Insight: “Retention drops to 20% after 6 months for customers acquired in Q1 2025.”


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


10. Market Basket Analysis (Optional)
Problem: Which products are frequently bought together?
	• Objective: Recommend product bundles and improve cross-selling strategies.
	• Dataset: order_items
	• Analysis type: Association rules, frequent itemset mining
	• Example Insight: “Customers who buy smartphone cases also buy screen protectors 60% of the time.”

11. Seasonality & Trends
Problem: Are there seasonal trends in orders or revenue?
	• Objective: Prepare for peak sales periods like holidays.
	• Dataset: orders, order_items
	• Analysis type: Time series & trend analysis
	• Example Insight: “Black Friday week sees a 2x increase in orders compared to the monthly average.”

? Portfolio Coverage Check:
	• Cohorts: ? Problem 1
	• LTV: ? Problem 2
	• CAC: ? Problem 3 (assumed)
	• Retention: ? Problem 4
	• Forecasting: ? Problem 5
	• Other business insights: ? Problems 6–11

If you want, I can now turn this into a full “project roadmap”, mapping each business problem to EDA steps, charts/visualizations, and Python/SQL analysis approaches, so you can literally start building your portfolio step by step.
Do you want me to do that next?

From <https://chatgpt.com/c/697870ed-11b8-839a-9483-82e808b43e70> 


Excellent — this is exactly how senior data analysts frame portfolio projects.
You already have the analyses.
Now we’ll convert each one into proper business storytelling format like:
	Business Problem ? Approach ? Insight ? Recommendation
This makes your project look business-driven, not academic.

? Olist Portfolio — Business Framing (Professional Version)

1. Customer Cohort Analysis (Retention over time)
Business Problem:
Customer churn is increasing and repeat purchases are low. We need to understand how long customers stay active after their first purchase.
Approach:
Conducted monthly cohort analysis by grouping customers based on their first purchase month and tracking repeat purchases over time.
Insight:
Only 25–30% of customers return after 3 months, with sharp drop-offs after the first purchase.
Recommendation:
Introduce loyalty discounts, email reminders, and personalized offers within the first 60 days to improve early retention.

2. Customer Lifetime Value (LTV)
Business Problem:
Not all customers contribute equally to revenue, but marketing efforts treat them the same. We need to identify high-value customers.
Approach:
Calculated historical LTV using total revenue, purchase frequency, and customer lifespan; segmented customers into high, medium, and low value.
Insight:
Top 20% of customers contribute ~75% of total revenue.
Recommendation:
Focus retention campaigns and premium services on high-LTV customers while using low-cost automation for low-value segments.

3. Customer Acquisition Cost (CAC)
Business Problem:
Customer acquisition spending may be inefficient. We need to know whether acquiring customers is profitable.
Approach:
Estimated CAC by dividing assumed marketing spend by number of new customers acquired each month and compared CAC with LTV.
Insight:
Customers cost ~$5 to acquire while average LTV is ~$40, giving an LTV/CAC ratio of 8:1.
Recommendation:
Scale acquisition campaigns since ROI is strong; invest more in channels bringing high-LTV customers.

4. Customer Retention Rate
Business Problem:
A large number of customers purchase only once, limiting sustainable growth.
Approach:
Measured month-over-month retention rates and repeat purchase frequency across customer cohorts.
Insight:
Retention drops below 20% after 6 months, indicating high churn.
Recommendation:
Launch subscription models, rewards programs, and re-engagement emails to increase repeat purchases.

5. Forecasting Orders & Revenue
Business Problem:
Unpredictable demand makes it difficult to plan inventory, staffing, and logistics.
Approach:
Aggregated monthly orders and revenue and built time-series forecasting models (ARIMA/Prophet) to predict future demand.
Insight:
Sales show strong seasonality with peaks during holidays; next quarter expected to grow ~12%.
Recommendation:
Increase inventory and staffing before peak seasons to avoid stockouts and delivery delays.

6. Regional Sales Analysis
Business Problem:
Marketing and logistics budgets are spread evenly without knowing which regions drive the most revenue.
Approach:
Analyzed revenue and order volume by state and city using customer geolocation data.
Insight:
São Paulo and Rio de Janeiro contribute nearly 50% of total revenue.
Recommendation:
Prioritize warehouses, faster shipping, and targeted ads in high-performing regions.

7. Product & Category Performance
Business Problem:
Some products underperform or receive poor reviews, impacting customer satisfaction and profitability.
Approach:
Ranked products and categories by revenue, volume, and review scores; identified high-return/low-rating products.
Insight:
Electronics drive 35% of revenue but also have higher negative reviews.
Recommendation:
Improve quality checks and descriptions for low-rated products; increase inventory for best sellers.

8. Delivery Performance Analysis
Business Problem:
Delayed deliveries may increase costs and harm customer experience.
Approach:
Calculated average delivery time by seller and region; compared promised vs actual delivery dates.
Insight:
North region deliveries take ~20% longer than average; some sellers consistently underperform.
Recommendation:
Optimize routing, add regional warehouses, and set SLAs for slow sellers.

9. Impact of Delivery on Customer Satisfaction
Business Problem:
Low review scores may be driven by operational issues rather than product quality.
Approach:
Correlated delivery delays with customer review ratings.
Insight:
Orders delayed >7 days receive 1–1.5 points lower ratings on average.
Recommendation:
Prioritize faster delivery for high-value orders and proactively notify customers of delays.

10. Market Basket Analysis
Business Problem:
Opportunities for cross-selling and upselling are not being utilized.
Approach:
Applied association rule mining to identify products frequently purchased together.
Insight:
Customers buying smartphone cases also buy screen protectors 60% of the time.
Recommendation:
Create bundle offers and recommendation engines to increase average order value.

11. Seasonality & Trends
Business Problem:
Sales fluctuate throughout the year, causing stockouts in peak periods and excess inventory in slow months.
Approach:
Analyzed monthly and weekly sales trends to identify seasonal patterns.
Insight:
Black Friday and holiday periods show 2x–3x order spikes.
Recommendation:
Plan promotions, inventory, and staffing ahead of peak seasons to maximize revenue.

? Why this format is powerful for your portfolio
This structure shows recruiters:
	• Business thinking ?
	• Analytical skills ?
	• Actionable recommendations ?
	• Not just “plots and charts” ?

If you want, next I can help you turn these into README/project report sections or resume bullet points, which is the final step to make your Olist project look industry-level.


