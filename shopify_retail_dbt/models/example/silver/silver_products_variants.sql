{{ config(materialized='table') }}

WITH bronze_products_cte AS (
    SELECT
        product_id,
        extract_date,
        ingested_at,
        source_file_path,
        raw_payload
    FROM `retail-data-warehouse-project.retail_bronze.shopify_products_raw`
),
variants_cte AS (
    SELECT
        product_id,
        extract_date,
        ingested_at,
        source_file_path,
        variant
    FROM bronze_products_cte, UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.variants')) AS variant
),
ranked AS (
    SELECT
        product_id,
        extract_date,
        ingested_at,
        source_file_path,

        json_value(variant, '$.id') AS variant_id,
        json_value(variant, '$.title') AS variant_title,
        cast(json_value(variant, '$.price') AS NUMERIC) AS variant_price,
        cast(json_value(variant, '$.position') AS INT64) AS variant_position,

        json_value(variant, '$.inventory_policy') AS inventory_policy,
        cast(json_value(variant, '$.compare_at_price') AS NUMERIC) AS compare_at_price,

        json_value(variant, '$.option1') AS option1,
        json_value(variant, '$.option2') AS option2,
        json_value(variant, '$.option3') AS option3,

        cast(json_value(variant, '$.created_at') AS TIMESTAMP) AS created_at,
        cast(json_value(variant, '$.updated_at') AS TIMESTAMP) AS updated_at,

        cast(json_value(variant, '$.taxable') AS BOOL) AS taxable,
        json_value(variant, '$.fulfillment_service') AS fulfillment_service,
        cast(json_value(variant, '$.grams') AS INT64) AS grams,
        json_value(variant, '$.inventory_management') AS inventory_management,
        cast(json_value(variant, '$.requires_shipping') AS BOOL) AS requires_shipping,

        json_value(variant, '$.sku') AS sku,
        cast(json_value(variant, '$.weight') AS FLOAT64) AS variant_weight,
        json_value(variant, '$.weight_unit') AS weight_unit,

        json_value(variant, '$.inventory_item_id') AS inventory_item_id,
        cast(json_value(variant, '$.inventory_quantity') AS INT64) AS inventory_quantity,

        ROW_NUMBER() OVER (
            PARTITION BY json_value(variant, '$.id')
            ORDER BY extract_date DESC
        ) AS rn

    FROM variants_cte
)

SELECT
    product_id,
    extract_date,
    ingested_at,
    source_file_path,
    variant_id,
    variant_title,
    variant_price,
    variant_position,
    inventory_policy,
    compare_at_price,
    option1,
    option2,
    option3,
    created_at,
    updated_at,
    taxable,
    fulfillment_service,
    grams,
    inventory_management,
    requires_shipping,
    sku,
    variant_weight,
    weight_unit,
    inventory_item_id,
    inventory_quantity

FROM ranked
WHERE rn = 1
