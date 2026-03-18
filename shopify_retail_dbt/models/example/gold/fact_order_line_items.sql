{{ config(
    materialized='table',
    schema='retail_gold'
) }}

SELECT
    lio.line_item_id,
    lio.order_id,
    o.customer_id,
    lio.product_id,
    lio.variant_id,
    DATE(o.created_at) AS order_date,

    lio.quantity,
    lio.unit_price,
    lio.quantity * lio.unit_price AS line_revenue,

    lio.fulfillment_status,
    lio.requires_shipping,
    lio.taxable

FROM {{ ref('silver_order_line_items') }} AS lio
INNER JOIN {{ ref('silver_orders') }} AS o
    ON lio.order_id = o.order_id