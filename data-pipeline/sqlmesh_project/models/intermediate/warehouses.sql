MODEL (
    name intermediate.warehouses,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key warehouse_id
    )
);

SELECT
    warehouse_id::INT AS warehouse_id,
    warehouse_name::TEXT AS warehouse_name,
    warehouse_address::TEXT AS warehouse_address,
    region_id::INT AS region_id,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.warehouses
