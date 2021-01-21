DROP TABLE IF EXISTS _SESSION.rawdata_stage;

CREATE TEMPORARY TABLE _SESSION.rawdata_stage
(
	country STRING NOT NULL,
	cases_timestamp TIMESTAMP,
	previous_confirmed_cases INT64,
	cumulative_confirmed_cases INT64,
	previous_deaths INT64,
	cumulative_deaths INT64,
	previous_recovered_cases INT64,
	cumulative_recovered_cases INT64,
	active_cases INT64
);

-- State wise data for a lot of countries is not available from the beginning
-- Rollup to country to avoid dany data issues
INSERT INTO _SESSION.rawdata_stage
SELECT
	sub.country,
	sub.cases_timestamp,
	LAG(sub.cumulative_confirmed_cases, 1, 0) OVER (
		PARTITION BY sub.country
		ORDER BY sub.cases_timestamp ASC
	) AS previous_confirmed_cases,		 
	sub.cumulative_confirmed_cases,
	LAG(sub.cumulative_deaths, 1, 0) OVER (
		PARTITION BY sub.country
		ORDER BY sub.cases_timestamp ASC
	) AS previous_deaths,	
	sub.cumulative_deaths,
	LAG(sub.cumulative_recovered_cases, 1, 0) OVER (
		PARTITION BY sub.country
		ORDER BY sub.cases_timestamp ASC
	) AS previous_recovered_cases,	
	sub.cumulative_recovered_cases,
	sub.active_cases
FROM (
	SELECT
		TIMESTAMP_TRUNC(rd.cases_timestamp, DAY) AS cases_timestamp,
		rd.country,
		SUM(rd.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
		SUM(rd.cumulative_deaths) AS cumulative_deaths,					
		SUM(rd.cumulative_recovered_cases) AS cumulative_recovered_cases,			
		SUM(rd.active_cases) AS active_cases
	FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped rd
	GROUP BY 1, 2
) sub;

DROP TABLE IF EXISTS _SESSION.rawdata_transform;

CREATE TEMPORARY TABLE _SESSION.rawdata_transform
(
	country STRING NOT NULL,
	cases_timestamp TIMESTAMP,
	new_confirmed_cases INT64,
	cumulative_confirmed_cases INT64,
	new_deaths INT64,
	cumulative_deaths INT64,
	new_recovered_cases INT64,
	cumulative_recovered_cases INT64,
	active_cases INT64
);

INSERT INTO _SESSION.rawdata_transform
-- Fix cumulative data issues
-- Cumulative data should always increase, if not pick previous value
WITH cumulative_data_fix AS (
	SELECT
		rt.country,
		rt.cases_timestamp,
		rt.previous_confirmed_cases,
		CASE WHEN rt.cumulative_confirmed_cases >= rt.previous_confirmed_cases
			 THEN rt.cumulative_confirmed_cases
			 ELSE rt.previous_confirmed_cases
		END AS cumulative_confirmed_cases,
		rt.previous_deaths,
		CASE WHEN rt.cumulative_deaths >= rt.previous_deaths
			 THEN rt.cumulative_deaths
			 ELSE rt.previous_deaths
		END AS cumulative_deaths,
		rt.previous_recovered_cases,
		CASE WHEN rt.cumulative_recovered_cases >= rt.previous_recovered_cases
			 THEN rt.cumulative_recovered_cases
			 ELSE rt.previous_recovered_cases
		END AS cumulative_recovered_cases,
		rt.active_cases
	FROM _SESSION.rawdata_stage rt
)
SELECT
	cdf.country,
	TIMESTAMP_TRUNC(cdf.cases_timestamp, DAY) AS cases_timestamp,
	(cdf.cumulative_confirmed_cases - cdf.previous_confirmed_cases) AS new_confirmed_cases,
	cdf.cumulative_confirmed_cases,
	(cdf.cumulative_deaths - cdf.previous_deaths) AS new_deaths,	
	cdf.cumulative_deaths,
	(cdf.cumulative_recovered_cases - cdf.previous_recovered_cases) AS new_recovered_cases,	
	cdf.cumulative_recovered_cases,
	cdf.active_cases
FROM cumulative_data_fix cdf;

TRUNCATE TABLE abar_bq_dataset_covid19_dw_bi.fact_daily_country_covid_metrics;

INSERT INTO abar_bq_dataset_covid19_dw_bi.fact_daily_country_covid_metrics
WITH country_dim AS (
	SELECT DISTINCT
		rd.country,
		rd.country_code_iso2,
		rd.country_code_iso3
	FROM abar_bq_dataset_covid19_dw_bi.region_dim rd
), 
daily_covid_metrics AS (
	SELECT
		sub.cases_timestamp,
		sub.country_code_iso2,
		sub.country_code_iso3,
		sub.new_confirmed_cases,
		sub.cumulative_confirmed_cases,
		AVG(sub.new_confirmed_cases) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
		) AS _7day_avg_confirmed_cases,
		LAG(sub.new_confirmed_cases, 7, 0) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
		) AS last_week_confirmed_cases,						
		sub.new_deaths,
		sub.cumulative_deaths,
		AVG(sub.new_deaths) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
		) AS _7day_avg_deaths,
		LAG(sub.new_deaths, 7, 0) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
		) AS last_week_deaths,						
		sub.new_recovered_cases,
		sub.cumulative_recovered_cases,	
		sub.active_cases
	FROM (
		SELECT
			rt.cases_timestamp,
			cd.country_code_iso2,
			cd.country_code_iso3,
			SUM(rt.new_confirmed_cases) AS new_confirmed_cases,
			SUM(rt.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
			SUM(rt.new_deaths) AS new_deaths,
			SUM(rt.cumulative_deaths) AS cumulative_deaths,					
			SUM(rt.new_recovered_cases) AS new_recovered_cases,	
			SUM(rt.cumulative_recovered_cases) AS cumulative_recovered_cases,			
			SUM(rt.active_cases) AS active_cases
		FROM _SESSION.rawdata_transform rt
		INNER JOIN country_dim cd
			ON UPPER(TRIM(rt.country)) = UPPER(TRIM(cd.country))
		GROUP BY
			rt.cases_timestamp,
			cd.country_code_iso2, 
			cd.country_code_iso3
	) sub
)
SELECT
	dcm.cases_timestamp,
	dcm.country_code_iso2,
	dcm.country_code_iso3,
	CAST(dcm.new_confirmed_cases AS INT64) AS new_confirmed_cases,
	CAST(dcm.cumulative_confirmed_cases AS INT64) AS cumulative_confirmed_cases,
	CAST(dcm._7day_avg_confirmed_cases AS INT64) AS _7day_avg_confirmed_cases,
	ROUND(CAST(((dcm.new_confirmed_cases - dcm.last_week_confirmed_cases)/NULLIF(dcm.last_week_confirmed_cases,0))*100
		AS FLOAT64),2) AS _7day_percent_change_confirmed_cases,
	CAST(dcm.new_deaths AS INT64) AS new_deaths,
	CAST(dcm.cumulative_deaths AS INT64) AS cumulative_deaths,
	CAST(dcm._7day_avg_deaths AS INT64) AS _7day_avg_deaths,
	ROUND(CAST(((dcm.new_deaths - dcm.last_week_deaths)/NULLIF(dcm.last_week_deaths,0))*100
		AS FLOAT64),2) AS _7day_percent_change_deaths,
	CAST(dcm.new_recovered_cases AS INT64) AS new_recovered_cases,
	CAST(dcm.cumulative_recovered_cases AS INT64) AS cumulative_recovered_cases,
	CAST(dcm.active_cases AS INT64) AS active_cases,
	ROUND(CAST(((dcm.cumulative_recovered_cases/NULLIF(dcm.cumulative_confirmed_cases,0))*100) 
		AS FLOAT64),2) AS recovery_rate,	
	ROUND(CAST(((dcm.cumulative_deaths/NULLIF(dcm.cumulative_confirmed_cases,0))*100) 
		AS FLOAT64),2) AS case_fatality_ratio
FROM daily_covid_metrics dcm
;

DROP TABLE IF EXISTS _SESSION.rawdata_transform;
DROP TABLE IF EXISTS _SESSION.rawdata_stage;