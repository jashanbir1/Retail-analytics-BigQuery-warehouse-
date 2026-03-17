{{ config(materialized='table') }}

WITH bronze_orders_cte AS (
    SELECT
        order_id,
        extract_date,
        ingested_at,
        source_file_path,
        raw_payload
    FROM `retail-data-warehouse-project.retail_bronze.shopify_orders_raw`
),
line_items_cte AS (
    SELECT
        order_id,
        extract_date,
        ingested_at,
        source_file_path,
        line_item
    FROM bronze_orders_cte, UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.line_items')) as line_item
)

SELECT
    order_id,
    extract_date,
    ingested_at,
    source_file_path,

    JSON_VALUE(line_item, '$.id') AS line_item_id,
    JSON_VALUE(line_item, '$.product_id') AS product_id,
    JSON_VALUE(line_item, '$.variant_id') AS variant_id,
    JSON_VALUE(line_item, '$.sku') AS sku,

    JSON_VALUE(line_item, '$.title') AS product_title,
    JSON_VALUE(line_item, '$.vendor') AS vendor,
    JSON_VALUE(line_item, '$.variant_title') AS variant_title,
    JSON_VALUE(line_item, '$.fulfillment_status') AS fulfillment_status,

    CAST(JSON_VALUE(line_item, '$.quantity') AS INT64) AS quantity,
    CAST(JSON_VALUE(line_item, '$.price') AS NUMERIC) AS unit_price,
    CAST(JSON_VALUE(line_item, '$.grams') AS INT64) AS grams,

    CAST(JSON_VALUE(line_item, '$.requires_shipping') AS BOOL) AS requires_shipping,
    CAST(JSON_VALUE(line_item, '$.taxable') AS BOOL) AS taxable,
    CAST(JSON_VALUE(line_item, '$.gift_card') AS BOOL) AS gift_card

FROM line_items_cte