CREATE OR REPLACE TABLE abar_bq_dataset_covid19_dw_bi.region_dim
(
	city STRING,
	lat_city FLOAT64,
	lng_city FLOAT64,
	country STRING,
	country_code_iso2 STRING,
	country_code_iso3 STRING,
	lat_country FLOAT64,
	lng_country FLOAT64,	
	state_province STRING,
	capital STRING,
	city_population INT64
)
CLUSTER BY
	country, state_province, city;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_dw_bi.date_dim
(
	calendar_timestamp TIMESTAMP,
	calendar_date DATE,
	year_timestamp TIMESTAMP,	
	year_number INT64,
	quarter_timestamp TIMESTAMP,	
	quarter_number INT64,
	quarter_string STRING,	
	month_timestamp TIMESTAMP,	
	month_number INT64,
	week_timestamp TIMESTAMP,	
	week_number INT64,
	day_number INT64
)
PARTITION BY
	TIMESTAMP_TRUNC(calendar_timestamp, DAY)
CLUSTER BY
	quarter_timestamp, month_timestamp, week_timestamp, calendar_date;