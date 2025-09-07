MODEL (
    name intermediate.products,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key product_id
    )
);

SELECT
    product_id::TEXT AS product_id,
    product_name::TEXT AS product_name,
    category_name::TEXT AS category_name,
    product_description::TEXT AS product_description,
    product_standard_cost::DOUBLE AS product_standard_cost,
    product_list_price::DOUBLE AS product_list_price,
    profit::DOUBLE AS profit,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.products
