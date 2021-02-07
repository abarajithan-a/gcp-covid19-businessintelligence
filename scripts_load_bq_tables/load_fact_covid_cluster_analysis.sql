DROP TABLE IF EXISTS _SESSION.rawdata_stage;

CREATE TEMPORARY TABLE _SESSION.rawdata_stage
(
	cases_timestamp TIMESTAMP,
	country STRING NOT NULL,	
	lat FLOAT64,
	lng FLOAT64,
	geo_point GEOGRAPHY,
	precision INT64,
	cluster_description STRING,	
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
	sub.cases_timestamp,
	sub.country,
	sub.lat,
	sub.lng,
	ST_GEOGPOINT(sub.lng,sub.lat) AS geo_point,			
	sub.precision,
	sub.cluster_description,
	LAG(sub.cumulative_confirmed_cases, 1, 0) OVER (
		PARTITION BY sub.country, sub.lat_lng, sub.precision
		ORDER BY sub.cases_timestamp ASC
	) AS previous_confirmed_cases,		 
	sub.cumulative_confirmed_cases,
	LAG(sub.cumulative_deaths, 1, 0) OVER (
		PARTITION BY sub.country, sub.lat_lng, sub.precision
		ORDER BY sub.cases_timestamp ASC
	) AS previous_deaths,	
	sub.cumulative_deaths,
	LAG(sub.cumulative_recovered_cases, 1, 0) OVER (
		PARTITION BY sub.country, sub.lat_lng, sub.precision
		ORDER BY sub.cases_timestamp ASC
	) AS previous_recovered_cases,	
	sub.cumulative_recovered_cases,
	sub.active_cases
FROM (
	SELECT
		TIMESTAMP_TRUNC(rd.cases_timestamp, DAY) AS cases_timestamp,
		rd.country,		
		ROUND(SAFE_CAST(rd.lat AS FLOAT64),0) AS lat,
		ROUND(SAFE_CAST(rd.long AS FLOAT64),0) AS lng,
		0 AS precision,
    	SAFE_CAST(ROUND(SAFE_CAST(rd.lat AS FLOAT64),0) AS STRING) || ',' || 
    		SAFE_CAST(ROUND(SAFE_CAST(rd.long AS FLOAT64),0) AS STRING) AS lat_lng,
		'country or large region, 111.32 km' AS cluster_description,	
		SUM(rd.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
		SUM(rd.cumulative_deaths) AS cumulative_deaths,					
		SUM(rd.cumulative_recovered_cases) AS cumulative_recovered_cases,			
		SUM(rd.active_cases) AS active_cases
	FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped rd
	GROUP BY 1, 2, 3, 4, 5, 6, 7

	UNION ALL

	SELECT
		TIMESTAMP_TRUNC(rd.cases_timestamp, DAY) AS cases_timestamp,
		rd.country,		
		ROUND(SAFE_CAST(rd.lat AS FLOAT64),1) AS lat,
		ROUND(SAFE_CAST(rd.long AS FLOAT64),1) AS lng,
		1 AS precision,
    	SAFE_CAST(ROUND(SAFE_CAST(rd.lat AS FLOAT64),1) AS STRING) || ',' || 
    		SAFE_CAST(ROUND(SAFE_CAST(rd.long AS FLOAT64),1) AS STRING) AS lat_lng,  
		'large city or district, 11.132 km' AS cluster_description,	
		SUM(rd.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
		SUM(rd.cumulative_deaths) AS cumulative_deaths,					
		SUM(rd.cumulative_recovered_cases) AS cumulative_recovered_cases,			
		SUM(rd.active_cases) AS active_cases
	FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped rd
	GROUP BY 1, 2, 3, 4, 5, 6, 7

	UNION ALL

	SELECT
		TIMESTAMP_TRUNC(rd.cases_timestamp, DAY) AS cases_timestamp,
		rd.country,			
		ROUND(SAFE_CAST(rd.lat AS FLOAT64),2) AS lat,
		ROUND(SAFE_CAST(rd.long AS FLOAT64),2) AS lng,	
		2 AS precision,
    	SAFE_CAST(ROUND(SAFE_CAST(rd.lat AS FLOAT64),2) AS STRING) || ',' || 
    		SAFE_CAST(ROUND(SAFE_CAST(rd.long AS FLOAT64),2) AS STRING) AS lat_lng,  
		'town or village, 1.1132 km' AS cluster_description,	
		SUM(rd.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
		SUM(rd.cumulative_deaths) AS cumulative_deaths,					
		SUM(rd.cumulative_recovered_cases) AS cumulative_recovered_cases,			
		SUM(rd.active_cases) AS active_cases
	FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped rd
	GROUP BY 1, 2, 3, 4, 5, 6, 7

	UNION ALL

	SELECT
		TIMESTAMP_TRUNC(rd.cases_timestamp, DAY) AS cases_timestamp,
		rd.country,			
		ROUND(SAFE_CAST(rd.lat AS FLOAT64),3) AS lat,
		ROUND(SAFE_CAST(rd.long AS FLOAT64),3) AS lng,	
		3 AS precision,
    	SAFE_CAST(ROUND(SAFE_CAST(rd.lat AS FLOAT64),3) AS STRING) || ',' || 
    		SAFE_CAST(ROUND(SAFE_CAST(rd.long AS FLOAT64),3) AS STRING) AS lat_lng,  
		'neighborhood-street, 111.32 m' AS cluster_description,	
		SUM(rd.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
		SUM(rd.cumulative_deaths) AS cumulative_deaths,					
		SUM(rd.cumulative_recovered_cases) AS cumulative_recovered_cases,			
		SUM(rd.active_cases) AS active_cases
	FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped rd
	GROUP BY 1, 2, 3, 4, 5, 6, 7

	UNION ALL

	SELECT
		TIMESTAMP_TRUNC(rd.cases_timestamp, DAY) AS cases_timestamp,
		rd.country,		
		ROUND(SAFE_CAST(rd.lat AS FLOAT64),4) AS lat,
		ROUND(SAFE_CAST(rd.long AS FLOAT64),4) AS lng,		
		4 AS precision,
    	SAFE_CAST(ROUND(SAFE_CAST(rd.lat AS FLOAT64),4) AS STRING) || ',' || 
    		SAFE_CAST(ROUND(SAFE_CAST(rd.long AS FLOAT64),4) AS STRING) AS lat_lng,    
		'individual street - large buildings, 11.132 m' AS cluster_description,	
		SUM(rd.cumulative_confirmed_cases) AS cumulative_confirmed_cases,							
		SUM(rd.cumulative_deaths) AS cumulative_deaths,					
		SUM(rd.cumulative_recovered_cases) AS cumulative_recovered_cases,			
		SUM(rd.active_cases) AS active_cases
	FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped rd
	GROUP BY 1, 2, 3, 4, 5, 6, 7			
) sub;

TRUNCATE TABLE abar_bq_dataset_covid19_dw_bi.fact_covid_cluster_analysis;

INSERT INTO abar_bq_dataset_covid19_dw_bi.fact_covid_cluster_analysis
-- Fix cumulative data issues
-- Cumulative data should always increase, if not pick previous value
WITH cumulative_data_fix AS (
	SELECT
		rt.cases_timestamp,
		rt.country,
		rt.lat,
		rt.lng,
		rt.geo_point,			
		rt.precision,
		rt.cluster_description,	
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
), 
country_dim AS (
	SELECT DISTINCT
		rd.country,
		rd.country_code_iso2,
		rd.country_code_iso3
	FROM abar_bq_dataset_covid19_dw_bi.region_dim rd
)
SELECT
	TIMESTAMP_TRUNC(cdf.cases_timestamp, DAY) AS cases_timestamp,
	cd.country_code_iso2,
	cd.country_code_iso3,
	cdf.lat,
	cdf.lng,
	cdf.geo_point,			
	cdf.precision,
	cdf.cluster_description,
	(cdf.cumulative_confirmed_cases - cdf.previous_confirmed_cases) AS new_confirmed_cases,
	(cdf.cumulative_deaths - cdf.previous_deaths) AS new_deaths,	
	(cdf.cumulative_recovered_cases - cdf.previous_recovered_cases) AS new_recovered_cases,
	cdf.active_cases
FROM cumulative_data_fix cdf
INNER JOIN country_dim cd
	ON UPPER(TRIM(cdf.country)) = UPPER(TRIM(cd.country));

DROP TABLE IF EXISTS _SESSION.rawdata_stage;