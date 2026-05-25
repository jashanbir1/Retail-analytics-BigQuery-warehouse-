{{ config(materialized='table') }}

-- Deduplication: same customer may appear in multiple extract_date partitions; keep the latest version only.

WITH ranked AS (
    SELECT
        customer_id,
        extract_date,
        ingested_at,
        source_file_path,

        json_value(raw_payload, '$.first_name') AS first_name,
        json_value(raw_payload, '$.last_name') AS last_name,
        json_value(raw_payload, '$.email') AS email,
        json_value(raw_payload, '$.phone') AS phone_number,
        json_value(raw_payload, '$.state') AS state,

        cast(json_value(raw_payload, '$.orders_count') AS INT64) AS orders_count,
        cast(json_value(raw_payload, '$.total_spent') AS NUMERIC) AS total_spent,

        cast(json_value(raw_payload, '$.verified_email') AS BOOL) AS verified_email,
        cast(json_value(raw_payload, '$.tax_exempt') AS BOOL) AS tax_exempt,

        json_value(raw_payload, '$.last_order_name') AS last_order_name,

        json_value(raw_payload, '$.tags') AS tags,
        json_value(raw_payload, '$.currency') AS currency,

        cast(json_value(raw_payload, '$.created_at') AS TIMESTAMP) AS created_at,
        cast(json_value(raw_payload, '$.updated_at') AS TIMESTAMP) AS updated_at,

        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY extract_date DESC) AS rn

    FROM `retail-data-warehouse-project.retail_bronze.shopify_customers_raw`
)

SELECT
    customer_id,
    extract_date,
    ingested_at,
    source_file_path,
    first_name,
    last_name,
    email,
    phone_number,
    state,
    orders_count,
    total_spent,
    verified_email,
    tax_exempt,
    last_order_name,
    tags,
    currency,
    created_at,
    updated_at

FROM ranked
WHERE rn = 1
