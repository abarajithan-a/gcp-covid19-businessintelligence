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
	confirmed_cases INT64,
	deaths INT64,
	recovered_cases INT64,
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