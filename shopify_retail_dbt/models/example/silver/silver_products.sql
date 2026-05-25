{{ config(materialized='table') }}

-- Deduplication: same product may appear in multiple extract_date partitions; keep the latest version only.

WITH ranked AS (
    SELECT
        product_id,
        extract_date,
        ingested_at,
        source_file_path,

        json_value(raw_payload, '$.title') AS product_title,
        json_value(raw_payload, '$.handle') AS product_handle,
        json_value(raw_payload, '$.vendor') AS vendor,
        json_value(raw_payload, '$.product_type') AS product_type,
        json_value(raw_payload, '$.tags') AS tags,
        json_value(raw_payload, '$.status') AS product_status,

        cast(json_value(raw_payload, '$.created_at') AS TIMESTAMP) AS created_at,
        cast(json_value(raw_payload, '$.updated_at') AS TIMESTAMP) AS updated_at,
        cast(json_value(raw_payload, '$.published_at') AS TIMESTAMP) AS published_at,

        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY extract_date DESC) AS rn

    FROM `retail-data-warehouse-project.retail_bronze.shopify_products_raw`
)

SELECT
    product_id,
    extract_date,
    ingested_at,
    source_file_path,
    product_title,
    product_handle,
    vendor,
    product_type,
    tags,
    product_status,
    created_at,
    updated_at,
    published_at

FROM ranked
WHERE rn = 1
