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
    FROM bronze_products_cte, UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.variants')) as variant
)

SELECT 
    product_id,
    extract_date,
    ingested_at,
    source_file_path,

    json_value(variant, '$.id') as variant_id,
    json_value(variant, '$.title') as variant_title,
    cast(json_value(variant, '$.price') AS NUMERIC) as variant_price,
    cast(json_value(variant, '$.position') as INT64) as variant_position,

    json_value(variant, '$.inventory_policy') as inventory_policy,
    cast(json_value(variant, '$.compare_at_price') AS NUMERIC) as compare_at_price,

    json_value(variant, '$.option1') as option1,
    json_value(variant, '$.option2') as option2,
    json_value(variant, '$.option3') as option3,

    cast(json_value(variant, '$.created_at') as TIMESTAMP) as created_at,
    cast(json_value(variant, '$.updated_at') as TIMESTAMP) as updated_at,

    cast(json_value(variant, '$.taxable') as BOOL) as taxable,
    json_value(variant, '$.fulfillment_service') as fulfillment_service,
    cast(json_value(variant, '$.grams') as INT64) as grams,
    json_value(variant, '$.inventory_management') as inventory_management,
    cast(json_value(variant, '$.requires_shipping') as BOOL) as requires_shipping,

    json_value(variant, '$.sku') as sku,
    cast(json_value(variant, '$.weight') as FLOAT64) as variant_weight,
    json_value(variant, '$.weight_unit') as weight_unit,

    json_value(variant, '$.inventory_item_id') as inventory_item_id,
    cast(json_value(variant, '$.inventory_quantity') as INT64) as inventory_quantity

FROM variants_cte




