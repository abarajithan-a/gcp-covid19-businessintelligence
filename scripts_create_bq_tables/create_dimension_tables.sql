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