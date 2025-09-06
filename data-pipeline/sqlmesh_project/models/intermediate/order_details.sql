MODEL (
    name intermediate.order_details,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key order_details_id
    )
);

SELECT
    order_details_id::INT AS order_details_id,
    product_id::TEXT AS product_id,
    order_item_quantity::INT AS order_item_quantity,
    per_unit_price::DOUBLE AS per_unit_price,
    order_status::TEXT AS order_status,
    order_id::INT AS order_id,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.order_details
