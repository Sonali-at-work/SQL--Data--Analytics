--Basic EDA(Exploratory Data Analysis)
--Database exploration
--Explore all object in database
select * from INFORMATION_SCHEMA.TABLES
--Explore all columns in database
select * from INFORMATION_SCHEMA.columns

--Dimension Exploration
select distinct(customer_city) from gold.dim_customers  -- 3640 city
-- OR 
select count(distinct(customer_city)) from gold.dim_customers  -- 3640 city
select distinct(customer_state) from gold.dim_customers -- 27 states
-- OR
select count(distinct(customer_state)) from gold.dim_customers -- 27 states
select 
    count(*) as total_rows,
    count(customer_city) as non_null_cities
from gold.dim_customers;

select count(distinct(product_category)) from gold.dim_products -- 72 product_categories 

select distinct(seller_city) from gold.dim_sellers -- sellers belong to 611 different cities 
select distinct(seller_state) from gold.dim_sellers -- sellers belong to 23 different states 
-- NEED to REVIST again  the making of this dimension gold.dim_sellers 
--- note the grain of each table at the gold layer

select distinct order_status from gold.fact_orders -- 8 distinct types of order_status exist

select distinct type from gold.fact_payments -- 5 payment types

-- the oldest order and the latest order
--How many Years of data is available (2 years)
select 
min(order_purchase_timestamp)as min_date,
max(order_purchase_timestamp) as max_date ,
datediff(Year,min(order_purchase_timestamp),max(order_purchase_timestamp))
from gold.fact_orders

-- number of days purchase happen or no. of real business sales days
select 
    count(distinct cast(order_purchase_timestamp as date)) as active_days
from gold.fact_orders; -- active_days 629

-- The oldest and the latest review
select 
min(creation_date)as min_date,
max(creation_date) as max_date ,
datediff(Year,min(creation_date),max(creation_date))as diff_between_oldest_newest_review
from gold.fact_reviews

select min(score)as min_score,max(score)as max_score from gold.fact_reviews
--min_score is 1 and max_score is 5
