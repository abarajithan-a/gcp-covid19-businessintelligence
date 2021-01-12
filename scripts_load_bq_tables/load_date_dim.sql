TRUNCATE TABLE abar_bq_dataset_covid19_dw_bi.date_dim;

INSERT INTO abar_bq_dataset_covid19_dw_bi.date_dim
SELECT
  t AS calendar_timestamp,  
  EXTRACT(DATE FROM t) AS calendar_date,
  TIMESTAMP_TRUNC(t, YEAR) AS year_timestamp,  
  EXTRACT(YEAR FROM t) AS year_number,
  TIMESTAMP_TRUNC(t, QUARTER) AS quarter_timestamp, 
  CAST(FORMAT_TIMESTAMP('%Q', t) AS INT64) AS quarter_number,
  'Q' || FORMAT_TIMESTAMP('%Q', t) AS quarter_string,  
  TIMESTAMP_TRUNC(t, MONTH) AS month_timestamp, 
  CAST(FORMAT_TIMESTAMP('%m', t) AS INT64) AS month_number,
  TIMESTAMP_TRUNC(t, WEEK) AS week_timestamp,  
  CAST(FORMAT_TIMESTAMP('%W', t) AS INT64) AS week_number,
  CAST(FORMAT_TIMESTAMP('%d', t) AS INT64) AS day_number  
FROM (
  SELECT
    *
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY('2020-01-01 00:00:00', '2025-12-31 00:00:00', INTERVAL 1 DAY)) AS t 
);