{{ config(materialized='table', schema='retail_gold') }}

SELECT
    customer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as customer_full_name,
    email,
    phone_number,
    state as customer_state,
    verified_email,
    tax_exempt,
    tags,
    currency,
    created_at AS customer_created_at,
    updated_at AS customer_updated_at

FROM {{ ref('silver_customers') }}