CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped
(
	fingerprint INT64 NOT NULL,
	fips STRING,
	city STRING,
	state STRING,
	country STRING NOT NULL,
	cases_timestamp TIMESTAMP,
	lat FLOAT64,
	long FLOAT64,
	geo_point GEOGRAPHY, 
	cumulative_confirmed_cases INT64,
	cumulative_deaths INT64,
	cumulative_recovered_cases INT64,
	active_cases INT64,
	combined_key STRING,
	incident_rate NUMERIC,
	case_fatality_ratio NUMERIC,
	ingestion_timestamp TIMESTAMP NOT NULL,
	file_name STRING NOT NULL
)
PARTITION BY
	TIMESTAMP_TRUNC(cases_timestamp, DAY)
CLUSTER BY
	country, state, city;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.world_cities_raw
(
	city STRING,
	city_ascii STRING,
	lat STRING,
	lng STRING,
	country STRING,
	iso2 STRING,
	iso3 STRING,
	admin_name STRING,
	capital STRING,
	population STRING,
	id STRING
)
CLUSTER BY
	country, admin_name, city;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.countries_lat_long_raw
(
	country STRING,
	lat STRING,
	lng STRING,
	name STRING
)
CLUSTER BY
	country;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.countries_gdp_raw
(
	country_name STRING,
	country_code STRING,
	gdp_usd STRING
)
CLUSTER BY
	country_code;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.countries_landarea_raw
(
	country_name STRING,
	country_code STRING,
	landarea_sq_kms STRING
)
CLUSTER BY
	country_code;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.countries_population_raw
(
	country_name STRING,
	country_code STRING,
	total_population STRING
)
CLUSTER BY
	country_code;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_raw.countries_incomegroup_raw
(
	country_code STRING,
	region STRING,	
	income_group STRING,
	country_name STRING	
)
CLUSTER BY
	country_code;