## Top 10 COVID19 cases by country - Bar Chart Race Timeline

This bar chart race timeline report is designed using the publicly available cloud based flourish.studio. The dataset in consideration is from April 2020 to January 2021. 

*Report Link:*  
[https://flourish-user-preview.com/5097155/egd9_KT_P6El2aJMBvA3qHwxc6Zr2Um2-P0oOrLQ2LcJRGIxAdR7qSqIrD8qGa0h/](https://flourish-user-preview.com/5097155/egd9_KT_P6El2aJMBvA3qHwxc6Zr2Um2-P0oOrLQ2LcJRGIxAdR7qSqIrD8qGa0h/)  

![](../images/covid19_cases_race_timeline.gif)

*BigQuery SQL Code:*  

~~~~
SELECT
  sub.cases_date,
  sub.country, sub.cumulative_confirmed_cases
FROM (
SELECT
  EXTRACT(DATE FROM a.cases_timestamp) AS cases_date, b.country,
  a.cumulative_confirmed_cases,
  ROW_NUMBER() OVER (
    PARTITION BY EXTRACT(DATE FROM a.cases_timestamp)
    ORDER BY a.cumulative_confirmed_cases DESC ) AS day_rank
FROM abar_bq_dataset_covid19_dw_bi.fact_daily_country_covid_metrics a 
INNER JOIN
(
SELECT DISTINCT
  country_code_iso2,
  country 
FROM abar_bq_dataset_covid19_dw_bi.region_dim) b
ON a.country_code_iso2 = b.country_code_iso2 
) sub
WHERE sub.day_rank <= 20 AND cases_date >= '2020-04-01'
ORDER BY 1 ASC,3 DESC;
~~~~

To build Flourish studio race chart, the data has to be pivoted with "Dates" as columns and events as rows. Since BigQuery doesn't support pivoting at the moment, this step is done in Python    
    
*Python Code:* 

~~~~
import pandas as pd

df  = pd.read_csv("covid_numbers.csv")
df1 = df.pivot(index='country', columns='cases_date', values='cumulative_confirmed_cases')
df1.to_csv('covid_numbers_pivoted.csv')
~~~~