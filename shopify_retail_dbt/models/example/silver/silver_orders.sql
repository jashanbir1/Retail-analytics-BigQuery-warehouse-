{{ config(materialized='table') }}

SELECT 
    order_id,
    extract_date,
    ingested_at,
    source_file_path,
    
    JSON_VALUE(raw_payload, '$.customer.id') AS customer_id,
    JSON_VALUE(raw_payload, '$.name') AS order_name,
    CAST(JSON_VALUE(raw_payload, '$.order_number') AS INT64) AS order_number,

    JSON_VALUE(raw_payload, '$.email') AS email,
    JSON_VALUE(raw_payload, '$.phone') AS phone,
    JSON_VALUE(raw_payload, '$.currency') AS currency,

    JSON_VALUE(raw_payload, '$.financial_status') AS financial_status,
    JSON_VALUE(raw_payload, '$.fulfillment_status') AS fulfillment_status,

    CAST(JSON_VALUE(raw_payload, '$.subtotal_price') AS NUMERIC) AS subtotal_price,
    CAST(JSON_VALUE(raw_payload, '$.total_price') AS NUMERIC) AS total_price,
    CAST(JSON_VALUE(raw_payload, '$.total_tax') AS NUMERIC) AS total_tax,
    CAST(JSON_VALUE(raw_payload, '$.total_discounts') AS NUMERIC) AS total_discounts,

    CAST(JSON_VALUE(raw_payload, '$.total_weight') AS INT64) AS total_weight,

    JSON_VALUE(raw_payload, '$.tags') AS tags,
    JSON_VALUE(raw_payload, '$.source_name') AS source_name,

    CAST(JSON_VALUE(raw_payload, '$.created_at') AS TIMESTAMP) AS created_at,
    CAST(JSON_VALUE(raw_payload, '$.updated_at') AS TIMESTAMP) AS updated_at,
    CAST(JSON_VALUE(raw_payload, '$.processed_at') AS TIMESTAMP) AS processed_at,
    CAST(JSON_VALUE(raw_payload, '$.closed_at') AS TIMESTAMP) AS closed_at


FROM `retail-data-warehouse-project.retail_bronze.shopify_orders_raw`