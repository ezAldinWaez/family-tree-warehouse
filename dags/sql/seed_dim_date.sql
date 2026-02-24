-- Populate date dimension with dates from 1900 to 2100
WITH date_series AS (
  SELECT CAST(full_date AS DATE) AS full_date
  FROM generate_series(DATE '1900-01-01', DATE '2100-12-31', INTERVAL 1 DAY) AS t(full_date)
)
INSERT INTO dim_date (
  date_sk,
  full_date,
  day_of_month,
  month_of_year,
  month_name,
  quarter_of_year,
  year_num,
  day_of_week,
  day_name,
  is_weekend
)
SELECT
  CAST(strftime(d.full_date, '%Y%m%d') AS INTEGER) AS date_sk,
  d.full_date,
  CAST(EXTRACT(day FROM d.full_date) AS TINYINT) AS day_of_month,
  CAST(EXTRACT(month FROM d.full_date) AS TINYINT) AS month_of_year,
  strftime(d.full_date, '%B') AS month_name,
  CAST(EXTRACT(quarter FROM d.full_date) AS TINYINT) AS quarter_of_year,
  CAST(EXTRACT(year FROM d.full_date) AS SMALLINT) AS year_num,
  CAST(EXTRACT(dow FROM d.full_date) AS TINYINT) AS day_of_week,
  strftime(d.full_date, '%A') AS day_name,
  EXTRACT(dow FROM d.full_date) IN (5, 6) AS is_weekend
FROM date_series d
ON CONFLICT DO NOTHING;
