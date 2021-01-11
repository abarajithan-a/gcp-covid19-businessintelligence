TRUNCATE TABLE abar_bq_dataset_covid19_dw_bi.region_dim;

-- REGEXP_REPLACE NORMALIZE to remove accented characters
INSERT INTO abar_bq_dataset_covid19_dw_bi.region_dim
SELECT
	REGEXP_REPLACE(NORMALIZE(ct.city, NFD), r'\pM', '') AS city,
	CAST(ct.lat AS FLOAT64) AS lat_city,
	CAST(ct.lng AS FLOAT64) AS lng_city,
	REGEXP_REPLACE(NORMALIZE(ct.country, NFD), r'\pM', '') AS country,
	ct.iso2 AS country_code_iso2,
	ct.iso3 AS country_code_iso3,
	CAST(co.lat AS FLOAT64) AS lat_country,
	CAST(co.lng AS FLOAT64) AS lng_country,
	REGEXP_REPLACE(NORMALIZE(ct.admin_name, NFD), r'\pM', '') AS state_province,
	REGEXP_REPLACE(NORMALIZE(ct.capital, NFD), r'\pM', '') AS capital,	
	CAST(SAFE_CAST(ct.population AS FLOAT64) AS INT64) AS city_population,
FROM abar_bq_dataset_covid19_raw.world_cities_raw ct
LEFT JOIN abar_bq_dataset_covid19_raw.countries_lat_long_raw co
	ON LOWER(TRIM(ct.iso2)) = LOWER(TRIM(co.country));