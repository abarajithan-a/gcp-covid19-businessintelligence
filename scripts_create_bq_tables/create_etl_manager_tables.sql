CREATE OR REPLACE TABLE abar_bq_dataset_covid19_etl_manager.sql_jobs (
	table_name STRING,
	job_status STRING,
	data_last_processed_timestamp TIMESTAMP,
	job_run_date DATE,
	job_run_timestamp TIMESTAMP
)
CLUSTER BY
	table_name, job_status;

CREATE OR REPLACE TABLE abar_bq_dataset_covid19_etl_manager.daily_ingest_covid19_raw_bad_data (
	fips STRING,
	city STRING,
	state STRING,
	country STRING NOT NULL,
	last_updated TIMESTAMP,
	lat STRING,
	long STRING,
	confirmed_cases STRING,
	deaths STRING,
	recovered_cases STRING,
	active_cases STRING,
	combined_key STRING,
	incident_rate STRING,
	case_fatality_ratio STRING,
	ingestion_timestamp TIMESTAMP NOT NULL,
	file_name STRING NOT NULL
)
PARTITION BY
	TIMESTAMP_TRUNC(last_updated, DAY)
CLUSTER BY
	country, state, city;