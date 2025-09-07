MODEL (
    name intermediate.regions,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key region_id
    )
);

SELECT
    region_id::INT AS region_id,
    region_name::TEXT AS region_name,
    country_name::TEXT AS country_name,
    state::TEXT AS state,
    city::TEXT AS city,
    postal_code::TEXT AS postal_code,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.regions
