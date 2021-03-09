-- Insert job run into etl manager jobs table
INSERT INTO abar_bq_dataset_covid19_etl_manager.sql_jobs
SELECT
	'daily_covid19_rawdata_deduped' AS table_name,
	'started' AS job_status,
	NULL AS data_last_processed_timestamp,
	current_date AS job_run_date,
	current_timestamp AS job_run_timestamp
;

-- Identify bad records for data type conversion using SAFE_CAST
-- And move them to _bad_data table
INSERT INTO abar_bq_dataset_covid19_etl_manager.daily_ingest_covid19_raw_bad_data
WITH bad_records AS (
SELECT
	*
FROM abar_bq_dataset_covid19_raw.daily_ingest_covid19_raw
WHERE
	(country IS NULL) OR
	(last_updated IS NOT NULL AND SAFE_CAST(last_updated AS TIMESTAMP) IS NULL) OR
	(lat IS NOT NULL AND SAFE_CAST(lat AS NUMERIC) IS NULL) OR
	(long IS NOT NULL AND SAFE_CAST(long AS NUMERIC) IS NULL) OR
	(confirmed_cases IS NOT NULL AND 
		SAFE_CAST(SAFE_CAST(confirmed_cases AS NUMERIC) AS INT64) IS NULL) OR
	(deaths IS NOT NULL AND 
		SAFE_CAST(SAFE_CAST(deaths AS NUMERIC) AS INT64) IS NULL) OR
	(recovered_cases IS NOT NULL AND 
		SAFE_CAST(SAFE_CAST(recovered_cases AS NUMERIC) AS INT64) IS NULL) OR
	(active_cases IS NOT NULL AND 
		SAFE_CAST(SAFE_CAST(active_cases AS NUMERIC) AS INT64) IS NULL) OR
	(incident_rate IS NOT NULL AND SAFE_CAST(incident_rate AS NUMERIC) IS NULL) OR
	(case_fatality_ratio IS NOT NULL AND SAFE_CAST(case_fatality_ratio AS NUMERIC) IS NULL) 
)
SELECT *
FROM bad_records;

-- Temporary table to compute fingerprints based on location and cases reported date(last_updated)
-- and compute their rankings if they have duplicates
CREATE TEMPORARY TABLE _SESSION.rawdata_dedup
(
	fingerprint INT64 NOT NULL,
	fingerprint_rank INT64,
	fips STRING,
	city STRING,
	state STRING,
	country STRING NOT NULL,
	last_updated TIMESTAMP,
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
);

INSERT INTO _SESSION.rawdata_dedup
SELECT
	sub.fingerprint,
	ROW_NUMBER() OVER (
		PARTITION BY sub.fingerprint
		ORDER BY sub.ingestion_timestamp DESC
	) AS fingerprint_rank,
	sub.fips,
	sub.city,
	sub.state,
	sub.country,
	sub.last_updated,
	sub.lat,
	sub.long,
	sub.geo_point, 
	sub.cumulative_confirmed_cases,
	sub.cumulative_deaths,
	sub.cumulative_recovered_cases,
	sub.active_cases,
	sub.combined_key,
	sub.incident_rate,
	sub.case_fatality_ratio,
	sub.ingestion_timestamp,
	sub.file_name
FROM
	(SELECT
		FARM_FINGERPRINT(
			IFNULL(rd.fips,'UNKNOWN') ||		
			IFNULL(rd.city,'UNKNOWN') ||
			IFNULL(rd.state,'UNKNOWN') ||
			IFNULL(CASE WHEN UPPER(TRIM(rd.country)) = 'US'
			 			THEN 'United States'
			 			ELSE rd.country END
				,'UNKNOWN') ||
			rd.last_updated
		) AS fingerprint,
		rd.fips,
		rd.city,
		rd.state,
		CASE WHEN UPPER(TRIM(rd.country)) = 'US'
			 THEN 'United States'
			 ELSE rd.country
		END AS country,
		rd.last_updated,
		SAFE_CAST(rd.lat AS FLOAT64) AS lat,
		SAFE_CAST(rd.long AS FLOAT64) AS long,
		ST_GEOGPOINT(
			SAFE_CAST(rd.long AS FLOAT64), SAFE_CAST(rd.lat AS FLOAT64)
		) AS geo_point,
		SAFE_CAST(SAFE_CAST(rd.confirmed_cases AS NUMERIC) AS INT64) AS cumulative_confirmed_cases,
		SAFE_CAST(SAFE_CAST(rd.deaths AS NUMERIC) AS INT64) AS cumulative_deaths,
		SAFE_CAST(SAFE_CAST(rd.recovered_cases AS NUMERIC) AS INT64) AS cumulative_recovered_cases,
		SAFE_CAST(SAFE_CAST(rd.active_cases AS NUMERIC) AS INT64) AS active_cases,
		rd.combined_key,
		SAFE_CAST(rd.incident_rate AS NUMERIC) AS incident_rate,
		SAFE_CAST(rd.case_fatality_ratio AS NUMERIC) AS case_fatality_ratio,
		rd.ingestion_timestamp,
		rd.file_name
	FROM abar_bq_dataset_covid19_raw.daily_ingest_covid19_raw rd
	WHERE
		(rd.country IS NOT NULL) AND
		(rd.last_updated IS NOT NULL AND SAFE_CAST(rd.last_updated AS TIMESTAMP) IS NOT NULL) AND
		(rd.lat IS NULL OR 
			(rd.lat IS NOT NULL AND SAFE_CAST(rd.lat AS NUMERIC) IS NOT NULL)) AND
		(rd.long IS NULL OR 
			(rd.long IS NOT NULL AND SAFE_CAST(rd.long AS NUMERIC) IS NOT NULL)) AND
		(rd.confirmed_cases IS NULL OR
			(rd.confirmed_cases IS NOT NULL AND SAFE_CAST(SAFE_CAST(rd.confirmed_cases AS NUMERIC) AS INT64) IS NOT NULL)) AND
		(rd.deaths IS NULL OR
			(rd.deaths IS NOT NULL AND SAFE_CAST(SAFE_CAST(rd.deaths AS NUMERIC) AS INT64) IS NOT NULL)) AND
		(rd.recovered_cases IS NULL OR
			(rd.recovered_cases IS NOT NULL AND SAFE_CAST(SAFE_CAST(rd.recovered_cases AS NUMERIC) AS INT64) IS NOT NULL)) AND
		(rd.active_cases IS NULL OR
			(rd.active_cases IS NOT NULL AND SAFE_CAST(SAFE_CAST(rd.active_cases AS NUMERIC) AS INT64) IS NOT NULL)) AND
		(rd.incident_rate IS NULL OR 
			(rd.incident_rate IS NOT NULL AND SAFE_CAST(rd.incident_rate AS NUMERIC) IS NOT NULL)) AND
		(rd.case_fatality_ratio IS NULL OR 
			(rd.case_fatality_ratio IS NOT NULL AND SAFE_CAST(rd.case_fatality_ratio AS NUMERIC) IS NOT NULL))
	) sub
;

-- Delete the old fingerprints
DELETE FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped
WHERE fingerprint IN (SELECT fingerprint FROM _SESSION.rawdata_dedup);

-- Insert data based on fingerprint ranking = 1
INSERT INTO abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped
SELECT
	rd.fingerprint,
	rd.fips,
	rd.city,
	rd.state,
	rd.country,
	rd.last_updated AS cases_timestamp,
	rd.lat,
	rd.long,
	rd.geo_point, 
	rd.cumulative_confirmed_cases,
	rd.cumulative_deaths,
	rd.cumulative_recovered_cases,
	rd.active_cases,
	rd.combined_key,
	rd.incident_rate,
	rd.case_fatality_ratio,
	rd.ingestion_timestamp,
	rd.file_name
FROM _SESSION.rawdata_dedup rd
WHERE rd.fingerprint_rank = 1;

-- Drop the temporary tables
DROP TABLE IF EXISTS _SESSION.rawdata_dedup;

TRUNCATE TABLE abar_bq_dataset_covid19_raw.daily_ingest_covid19_raw;

-- Update job run in etl manager jobs table
UPDATE abar_bq_dataset_covid19_etl_manager.sql_jobs
SET
	job_status = 'completed',
	data_last_processed_timestamp = (SELECT MAX(ingestion_timestamp) 
										FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped),
	job_run_timestamp = current_timestamp
WHERE 
	table_name = 'daily_covid19_rawdata_deduped' AND
	job_status = 'started' AND
	job_run_date = current_date
;