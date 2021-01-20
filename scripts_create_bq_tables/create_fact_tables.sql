CREATE OR REPLACE TABLE abar_bq_dataset_covid19_dw_bi.fact_daily_country_covid_metrics
(
	cases_timestamp TIMESTAMP,
	country_code_iso2 STRING,
	country_code_iso3 STRING,
	new_confirmed_cases INT64,
	cumulative_confirmed_cases INT64,
	7day_avg_confirmed_cases INT64,
	7day_percent_change_confirmed_cases FLOAT64,			
	deaths INT64,
	cumulative_deaths INT64,
	7day_avg_deaths INT64,
	7day_percent_change_deaths FLOAT64,			
	recovered_cases INT64,
	cumulative_recovered_cases INT64,	
	active_cases INT64,
	case_fatality_ratio FLOAT64
)
PARTITION BY
	TIMESTAMP_TRUNC(cases_timestamp, DAY)
CLUSTER BY
	country_code_iso2;