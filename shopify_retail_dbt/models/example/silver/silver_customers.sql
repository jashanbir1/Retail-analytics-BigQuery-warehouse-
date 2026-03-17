{{ config(materialized='table') }}

SELECT
    customer_id,
    extract_date,
    ingested_at,
    source_file_path,

    json_value(raw_payload, '$.first_name') as first_name,
    json_value(raw_payload, '$.last_name') as last_name,
    json_value(raw_payload, '$.email') as email,
    json_value(raw_payload, '$.phone') as phone_number,
    json_value(raw_payload, '$.state') as state,

    cast(json_value(raw_payload, '$.orders_count') as INT64) as orders_count,
    cast(json_value(raw_payload, '$.total_spent') as NUMERIC) as total_spent,

    cast(json_value(raw_payload, '$.verified_email') as BOOL) as verified_email,
    cast(json_value(raw_payload, '$.tax_exempt') as BOOL) as tax_exempt,

    json_value(raw_payload, '$.last_order_name') as last_order_name,

    json_value(raw_payload, '$.tags') as tags,
    json_value(raw_payload, '$.currency') as currency,

    cast(json_value(raw_payload, '$.created_at') as TIMESTAMP) as created_at,
    cast(json_value(raw_payload, '$.updated_at') as TIMESTAMP) as updated_at

FROM `retail-data-warehouse-project.retail_bronze.shopify_customers_raw`


