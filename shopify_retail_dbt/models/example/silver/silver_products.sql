{{ config(materialized='table') }}

SELECT
    --these are out already pulled columns
    product_id,
    extract_date,
    ingested_at,
    source_file_path,
    --this goes into a json value (raw_payload) and returns the value at the fields specified. also renames those column names in json to the newly named value
    -- $ = start at the root of the JSON
	-- .title = go to the title field
    json_value(raw_payload, '$.title') AS product_title, 
    json_value(raw_payload, '$.handle') AS product_handle,
    json_value(raw_payload, '$.vendor') as vendor,
    json_value(raw_payload, '$.product_type') AS product_type,
    json_value(raw_payload, '$.tags') AS tags,
    json_value(raw_payload, '$.status') AS product_status,

    cast(json_value(raw_payload, '$.created_at') as TIMESTAMP) as created_at,
    cast(json_value(raw_payload, '$.updated_at') as TIMESTAMP) as updated_at,
    cast(json_value(raw_payload, '$.published_at') as TIMESTAMP) as published_at

FROM `retail-data-warehouse-project.retail_bronze.shopify_products_raw`

