{{ config(materialized='table') }}

WITH bronze_customers_cte AS (
    SELECT
        customer_id,
        extract_date,
        ingested_at,
        source_file_path,
        raw_payload
    FROM `retail-data-warehouse-project.retail_bronze.shopify_customers_raw`
),
addresses_cte AS (
    SELECT
        customer_id,
        extract_date,
        ingested_at,
        source_file_path,
        address
    FROM bronze_customers_cte,
    UNNEST(JSON_QUERY_ARRAY(raw_payload, '$.addresses')) as address
)

SELECT
    customer_id,
    extract_date,
    ingested_at,
    source_file_path,

    JSON_VALUE(address, '$.id') AS address_id,
    JSON_VALUE(address, '$.first_name') AS first_name,
    JSON_VALUE(address, '$.last_name') AS last_name,
    JSON_VALUE(address, '$.company') AS company,
    JSON_VALUE(address, '$.address1') AS address1,
    JSON_VALUE(address, '$.address2') AS address2,
    JSON_VALUE(address, '$.city') AS city,
    JSON_VALUE(address, '$.province') AS province,
    JSON_VALUE(address, '$.country') AS country,
    JSON_VALUE(address, '$.zip') AS zip,
    JSON_VALUE(address, '$.phone') AS phone,
    JSON_VALUE(address, '$.name') AS full_name,
    JSON_VALUE(address, '$.province_code') AS province_code,
    JSON_VALUE(address, '$.country_code') AS country_code,
    JSON_VALUE(address, '$.country_name') AS country_name,
    CAST(JSON_VALUE(address, '$.default') AS BOOL) AS is_default_address

FROM addresses_cte
