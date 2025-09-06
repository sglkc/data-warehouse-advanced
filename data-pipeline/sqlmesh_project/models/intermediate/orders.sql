MODEL (
    name intermediate.orders,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key order_id
    )
);

SELECT
    order_id::INT AS order_id,
    TO_DATE(order_date, 'YYYY/MM/DD') AS order_date,
    customer_id::INT AS customer_id,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.orders
