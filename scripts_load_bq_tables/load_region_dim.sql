TRUNCATE TABLE abar_bq_dataset_covid19_dw_bi.region_dim;

-- REGEXP_REPLACE NORMALIZE to replace accented characters with regular characters
INSERT INTO abar_bq_dataset_covid19_dw_bi.region_dim
WITH dedup_query AS ( 
	SELECT
		sub.fingerprint,
		sub.city,
		sub.city_lat,
		sub.city_lng,
		sub.city_population,	
		sub.country,
		sub.country_code_iso2,
		sub.country_code_iso3,
		sub.country_lat,
		sub.country_lng,
		sub.country_gdp_usd,
		sub.country_landarea_sq_kms,
		sub.country_population,
		sub.country_incomegroup,
		sub.country_region,					
		sub.state_province,
		sub.capital,
		ROW_NUMBER() OVER (
			PARTITION BY sub.fingerprint
			ORDER BY sub.city_population DESC
		) AS fingerprint_rank	
	FROM (
		SELECT
			REGEXP_REPLACE(NORMALIZE(ct.city, NFD), r'\pM', '') AS city,
			SAFE_CAST(ct.lat AS FLOAT64) AS city_lat,
			SAFE_CAST(ct.lng AS FLOAT64) AS city_lng,
			SAFE_CAST(SAFE_CAST(ct.population AS FLOAT64) AS INT64) AS city_population,	
			REGEXP_REPLACE(NORMALIZE(ct.country, NFD), r'\pM', '') AS country,
			ct.iso2 AS country_code_iso2,
			ct.iso3 AS country_code_iso3,
			SAFE_CAST(co.lat AS FLOAT64) AS country_lat,
			SAFE_CAST(co.lng AS FLOAT64) AS country_lng,
			SAFE_CAST(cg.gdp_usd AS NUMERIC) AS country_gdp_usd,
			SAFE_CAST(cl.landarea_sq_kms AS NUMERIC) AS country_landarea_sq_kms,
			SAFE_CAST(cp.total_population AS INT64) AS country_population,
			ci.income_group AS country_incomegroup,
			ci.region AS country_region,				
			REGEXP_REPLACE(NORMALIZE(ct.admin_name, NFD), r'\pM', '') AS state_province,
			REGEXP_REPLACE(NORMALIZE(ct.capital, NFD), r'\pM', '') AS capital,
			FARM_FINGERPRINT(	
				IFNULL(ct.city,'UNKNOWN') ||
				IFNULL(ct.admin_name,'UNKNOWN') ||
				IFNULL(ct.country,'UNKNOWN') ||							
				IFNULL(ci.region,'UNKNOWN')
			) AS fingerprint	
		FROM abar_bq_dataset_covid19_raw.world_cities_raw ct
		LEFT JOIN abar_bq_dataset_covid19_raw.countries_lat_long_raw co
			ON LOWER(TRIM(ct.iso2)) = LOWER(TRIM(co.country))
		LEFT JOIN abar_bq_dataset_covid19_raw.countries_gdp_raw cg
			ON LOWER(TRIM(ct.iso3)) = LOWER(TRIM(cg.country_code))
		LEFT JOIN abar_bq_dataset_covid19_raw.countries_landarea_raw cl
			ON LOWER(TRIM(ct.iso3)) = LOWER(TRIM(cl.country_code))
		LEFT JOIN abar_bq_dataset_covid19_raw.countries_population_raw cp
			ON LOWER(TRIM(ct.iso3)) = LOWER(TRIM(cp.country_code))
		LEFT JOIN abar_bq_dataset_covid19_raw.countries_incomegroup_raw ci
			ON LOWER(TRIM(ct.iso3)) = LOWER(TRIM(ci.country_code))
	) sub
)
SELECT
	dq.fingerprint,
	dq.city,
	dq.city_lat,
	dq.city_lng,
	dq.city_population,	
	dq.country,
	dq.country_code_iso2,
	dq.country_code_iso3,
	dq.country_lat,
	dq.country_lng,
	dq.country_gdp_usd,
	dq.country_landarea_sq_kms,
	dq.country_population,
	dq.country_incomegroup,
	dq.country_region,					
	dq.state_province,
	dq.capital
FROM dedup_query dq
WHERE dq.fingerprint_rank = 1;