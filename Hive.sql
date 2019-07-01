-- Databricks notebook source
create table stocks (date_ String, Ticker String, Open_Value Double, High_Value Double, Low_Value Double, Close_Value Double, Volume_for_the_day int) row format delimited fields terminated by ',';

-- COMMAND ----------

describe stocks;

-- COMMAND ----------

select * from stocks;

-- COMMAND ----------

describe  stocks;

-- COMMAND ----------

select * from stocks limit 10;

-- COMMAND ----------

select ticker,date,close,lag(close,1) over(partition by ticker order by ticker) as yesterday_price from default.stocks 
where ticker = 'GIS'

-- COMMAND ----------

select ticker,date,close,case(lead(close,1) over(partition by ticker order by ticker)-close)>0 when true then "higher" when false then "lesser" end as Changes from default.stocks where ticker = 'GIS'

-- COMMAND ----------

select ticker,first_value(high) over(partition by ticker) as first_high from default.stocks order by ticker

-- COMMAND ----------

select ticker,last_value(high) over(partition by ticker) as first_high from default.stocks order by ticker

-- COMMAND ----------

select distinct(ticker),count(ticker) over(partition by ticker) as cnt from stocks order by ticker

-- COMMAND ----------

select distinct(ticker),sum(close) over(partition by ticker) as Close_total from stocks order by ticker

-- COMMAND ----------

select ticker,date,volume_for_the_day,sum(volume_for_the_day) over(partition by ticker order by date) as running_total from stocks 
order by ticker

-- COMMAND ----------

select ticker,date,volume_for_the_day,(volume_for_the_day*100/(sum(volume_for_the_day) over(partition by ticker)))as percent from stocks 
order by ticker

-- COMMAND ----------

select distinct(ticker), min(close) over(partition by ticker) as minimum from stocks order by ticker

-- COMMAND ----------

select distinct(ticker), max(close) over(partition by ticker) as minimum from stocks order by ticker

-- COMMAND ----------

select distinct(ticker), avg(close) over(partition by ticker) as maximum from stocks order by ticker

-- COMMAND ----------

select distinct(ticker),close,rank() over(partition by ticker order by close) as closing from stocks 

-- COMMAND ----------

select ticker,close,row_number() over(partition by ticker order by close) as num from stocks

-- COMMAND ----------

select ticker,close,dense_rank() over(partition by ticker order by close) as closing from stocks

-- COMMAND ----------

select ticker,cume_dist() over(partition by ticker order by close) as cummulative from stocks

-- COMMAND ----------

select ticker,close,percent_rank() over(partition by ticker order by close) as closing from stocks

-- COMMAND ----------

select ticker,ntile(5) over(partition by ticker order by close ) as bucket from stocks 

-- COMMAND ----------

select * from stocks limit 10;

-- COMMAND ----------

select distinct(ticker) , date , open , high , low , close , volume_for_the_day from stocks;

-- COMMAND ----------

select distinct ticker ,date ,(concat(ticker , '-',date)) from stocks limit 10 ;

-- COMMAND ----------

select distinct(ticker) , length(ticker) from stocks limit 10;

-- COMMAND ----------

select distinct(ticker) , lower(ticker) from stocks limit 10;

-- COMMAND ----------

select distinct(ticker) , lpad(ticker,5,'s') from stocks limit 10;

-- COMMAND ----------

select distinct(ticker) , rpad(ticker,5,'s') from stocks limit 10;

-- COMMAND ----------

select ltrim('    test')

-- COMMAND ----------

select ltrim('test    ')

-- COMMAND ----------

select distinct ticker , (repeat(ticker , 2)) from stocks;

-- COMMAND ----------

select distinct ticker ,(reverse(ticker)) from stocks;

-- COMMAND ----------

select split('apple:stocks',':');

-- COMMAND ----------

select substr('View the basic AAPL stock chart',4 )

-- COMMAND ----------

select substr('View the basic AAPL stock chart',4 ,3)

-- COMMAND ----------

select instr('View the basic AAPL stock chart','b')

-- COMMAND ----------

select regexp_extract('foothebar', 'foo(.*?)(bar)', 0);

