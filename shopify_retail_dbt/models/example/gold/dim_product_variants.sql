{{ config(
    materialized='table',
    schema='retail_gold'
) }}

SELECT
    variant_id,
    product_id,
    variant_title,
    sku,
    variant_price,
    option1,
    option2,
    option3,
    requires_shipping,
    taxable,
    variant_weight AS variant_weight,
    weight_unit,
    inventory_item_id,
    inventory_quantity,
    created_at AS variant_created_at,
    updated_at AS variant_updated_at

FROM {{ ref('silver_products_variants') }}