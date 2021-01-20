DROP TABLE IF EXISTS _SESSION.rawdata_dedup;

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
	confirmed_cases INT64,
	deaths INT64,
	recovered_cases INT64,
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
	sub.confirmed_cases,
	sub.deaths,
	sub.recovered_cases,
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
		SAFE_CAST(SAFE_CAST(rd.confirmed_cases AS NUMERIC) AS INT64) AS confirmed_cases,
		SAFE_CAST(SAFE_CAST(rd.deaths AS NUMERIC) AS INT64) AS deaths,
		SAFE_CAST(SAFE_CAST(rd.recovered_cases AS NUMERIC) AS INT64) AS recovered_cases,
		SAFE_CAST(SAFE_CAST(rd.active_cases AS NUMERIC) AS INT64) AS active_cases,
		rd.combined_key,
		SAFE_CAST(rd.incident_rate AS NUMERIC) AS incident_rate,
		SAFE_CAST(rd.case_fatality_ratio AS NUMERIC) AS case_fatality_ratio,
		rd.ingestion_timestamp,
		rd.file_name
	FROM abar_bq_dataset_covid19_raw.daily_ingest_covid19_raw rd) sub
;

DROP TABLE IF EXISTS _SESSION.new_fingerprints;

-- Temporary table to store only new fingerprints and changed fingerprints
CREATE TEMPORARY TABLE _SESSION.new_fingerprints
(
	fingerprint INT64 NOT NULL
);

INSERT INTO _SESSION.new_fingerprints
SELECT 
	rd.fingerprint
FROM _SESSION.rawdata_dedup rd
LEFT JOIN abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped dcrd
	ON rd.fingerprint = dcrd.fingerprint
WHERE rd.fingerprint_rank = 1 AND 
	(dcrd.fingerprint IS NULL OR rd.ingestion_timestamp <> dcrd.ingestion_timestamp);	 

-- Delete the old fingerprints
DELETE FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped
WHERE fingerprint IN (SELECT fingerprint FROM _SESSION.new_fingerprints);

-- Insert new and changed cases data based on fingerprint ranking = 1
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
	rd.confirmed_cases,
	rd.deaths,
	rd.recovered_cases,
	rd.active_cases,
	rd.combined_key,
	rd.incident_rate,
	rd.case_fatality_ratio,
	rd.ingestion_timestamp,
	rd.file_name
FROM _SESSION.rawdata_dedup rd
INNER JOIN _SESSION.new_fingerprints fp
	ON rd.fingerprint = fp.fingerprint
WHERE rd.fingerprint_rank = 1;

-- Drop the temporary tables
DROP TABLE IF EXISTS _SESSION.new_fingerprints;
DROP TABLE IF EXISTS _SESSION.rawdata_dedup;