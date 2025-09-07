MODEL (
    name intermediate.customers,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key customer_id
    )
);

SELECT
    customer_id::INT AS customer_id,
    customer_name::TEXT AS customer_name,
    customer_email::TEXT AS customer_email,
    customer_phone::TEXT AS customer_phone,
    customer_address::TEXT AS customer_address,
    customer_credit_limit::DOUBLE AS customer_credit_limit,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.customers
