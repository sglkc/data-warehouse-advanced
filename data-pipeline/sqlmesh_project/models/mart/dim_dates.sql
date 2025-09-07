MODEL (
    name mart.dim_dates,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column full_date
    ),
    cron '@daily',
);

WITH date_range AS (
    SELECT
        @start_dt AS start_date,
        @end_dt AS end_date
),

date_series AS (
    /* Step 1: Generate series of dates */
    SELECT
        generate_series(dr.start_date, dr.end_date, interval '1 day')::date AS calendar_date
    FROM date_range dr
),

date_attributes AS (
    SELECT
        calendar_date AS full_date,
        /* Step 2: Extract attributes for each date */
        to_char(calendar_date, 'YYYYMMDD')::int AS date_key,         -- Surrogate Key (YYYYMMDD integer)
        EXTRACT(YEAR FROM calendar_date)::int AS year,               -- Year attributes
        EXTRACT(QUARTER FROM calendar_date)::int AS quarter_num,
        'Q' || EXTRACT(QUARTER FROM calendar_date)::text AS quarter_name,
        EXTRACT(MONTH FROM calendar_date)::int AS month_num,
        to_char(calendar_date, 'Mon') AS month_name_short,           -- Abbreviated month
        EXTRACT(DAY FROM calendar_date)::int AS day_of_month,
        EXTRACT(DOY FROM calendar_date)::int AS day_of_year,         -- Day of year (1–366)
        EXTRACT(WEEK FROM calendar_date)::int AS week_of_year,       -- ISO week
        EXTRACT(ISODOW FROM calendar_date)::int AS day_of_week_num,  -- 1=Monday, 7=Sunday
        to_char(calendar_date, 'Dy') AS day_of_week_name_short,      -- Abbreviated weekday
        CASE WHEN EXTRACT(ISODOW FROM calendar_date) IN (6,7) THEN true ELSE false END AS is_weekend,
        CASE WHEN EXTRACT(DOY FROM calendar_date) = 1 THEN true ELSE false END AS is_start_of_year,
        CASE WHEN calendar_date = (date_trunc('month', calendar_date) + interval '1 month - 1 day')::date THEN true ELSE false END AS is_end_of_month,
        CASE WHEN EXTRACT(DAY FROM calendar_date) = 1 THEN true ELSE false END AS is_start_of_month,
        CASE WHEN (EXTRACT(YEAR FROM calendar_date)::int % 4 = 0 AND EXTRACT(YEAR FROM calendar_date)::int % 100 != 0)
               OR (EXTRACT(YEAR FROM calendar_date)::int % 400 = 0) THEN true ELSE false END AS is_leap_year,
        to_char(calendar_date, 'YYYY-MM') AS year_month,
        to_char(calendar_date, 'MM/DD/YYYY') AS full_date_us_format,
        to_char(calendar_date, 'DD-MM-YYYY') AS full_date_eu_format,
        /* Fiscal year logic (assuming fiscal year starts July 1st) */
        CASE WHEN EXTRACT(MONTH FROM calendar_date) >= 7
             THEN EXTRACT(YEAR FROM calendar_date)::int
             ELSE (EXTRACT(YEAR FROM calendar_date)::int - 1) END AS fiscal_year,
        CASE WHEN EXTRACT(MONTH FROM calendar_date) >= 7
             THEN EXTRACT(MONTH FROM calendar_date)::int - 6
             ELSE EXTRACT(MONTH FROM calendar_date)::int + 6 END AS fiscal_month,
        CEIL(
            (CASE WHEN EXTRACT(MONTH FROM calendar_date) >= 7
                  THEN EXTRACT(MONTH FROM calendar_date)::int - 6
                  ELSE EXTRACT(MONTH FROM calendar_date)::int + 6 END) / 3.0
        )::int AS fiscal_quarter,
        now(@var_timezone) AS load_ts
    FROM date_series
)

SELECT *
FROM date_attributes
ORDER BY full_date;
