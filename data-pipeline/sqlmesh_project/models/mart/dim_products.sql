MODEL (
  name mart.dim_products,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key product_key
  ),
);

SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id ASC) AS product_key, -- Surrogate Key
    p.product_id::TEXT AS id,
    p.product_name::TEXT AS name,
    p.category_name::TEXT AS category,
    p.product_standard_cost::DOUBLE AS standard_price,
    p.product_list_price::DOUBLE AS list_price,
    p.profit::DOUBLE AS profit,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM intermediate.products AS p
