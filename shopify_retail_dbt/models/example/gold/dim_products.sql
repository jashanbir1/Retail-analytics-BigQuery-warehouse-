{{ config(materialized='table', schema='retail_gold') }}

SELECT 
    product_id,
    product_title,
    product_handle,
    vendor,
    product_type,
    tags,
    product_status,
    created_at as product_created_at,
    updated_at as product_updated_at,
    published_at as product_published_at

FROM {{ ref('silver_products') }}