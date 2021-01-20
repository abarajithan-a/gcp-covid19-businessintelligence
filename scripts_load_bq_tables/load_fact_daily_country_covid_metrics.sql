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
		sub.confirmed_cases AS new_confirmed_cases,
		SUM(sub.confirmed_cases) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS cumulative_confirmed_cases,
		AVG(sub.confirmed_cases) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS _7day_avg_confirmed_cases,
		LAG(sub.confirmed_cases, 7, 0) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
		) AS last_week_confirmed_cases,			
		sub.deaths,
		SUM(sub.deaths) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS cumulative_deaths,
		AVG(sub.deaths) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS _7day_avg_deaths,
		LAG(sub.deaths, 7, 0) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
		) AS last_week_deaths,			
		sub.recovered_cases,
		SUM(sub.recovered_cases) OVER (
			PARTITION BY sub.country_code_iso2
			ORDER BY sub.cases_timestamp ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS cumulative_recovered_cases,	
		sub.active_cases
	FROM (
		SELECT
			cases_timestamp,
			country_code_iso2,
			country_code_iso3,
			SUM(dd.confirmed_cases) AS confirmed_cases,			
			SUM(dd.deaths) AS deaths,		
			SUM(dd.recovered_cases) AS recovered_cases,	
			SUM(dd.active_cases) AS active_cases
		FROM abar_bq_dataset_covid19_raw.daily_covid19_rawdata_deduped dd
		INNER JOIN country_dim cd
			ON UPPER(TRIM(dd.country)) = UPPER(TRIM(cd.country))
		GROUP BY
			dd.cases_timestamp,
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
	CAST(dcm.deaths AS INT64) AS deaths,
	CAST(dcm.cumulative_deaths AS INT64) AS cumulative_deaths,
	CAST(dcm._7day_avg_deaths AS INT64) AS _7day_avg_deaths,
	ROUND(CAST(((dcm.deaths - dcm.last_week_deaths)/NULLIF(dcm.last_week_deaths,0))*100
		AS FLOAT64),2) AS _7day_percent_change_deaths,
	CAST(dcm.recovered_cases AS INT64) AS recovered_cases,
	CAST(dcm.cumulative_recovered_cases AS INT64) AS cumulative_recovered_cases,
	CAST(dcm.active_cases AS INT64) AS active_cases,
	ROUND(CAST(((dcm.cumulative_deaths/NULLIF(dcm.cumulative_confirmed_cases,0))*100) 
		AS FLOAT64),2) AS case_fatality_ratio
FROM daily_covid_metrics dcm
;