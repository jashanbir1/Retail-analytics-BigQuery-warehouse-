{{ config(
    materialized='table',
    schema='retail_gold'
) }}

-- One row per order. Summarizes basket/order behavior by showing how many line items were in the order,
-- total units purchased, total order revenue, and whether the order was fulfilled.
SELECT
    fct.order_id,
    fct.customer_id,
    fct.order_date,
    COUNT(fct.line_item_id) AS line_item_count,
    SUM(fct.quantity) AS total_units_in_order,
    SUM(fct.line_revenue) AS order_revenue,
    MAX(fct.fulfillment_status) AS fulfillment_status,
    CASE
        WHEN MAX(fct.fulfillment_status) = 'fulfilled' THEN TRUE
        ELSE FALSE
    END AS is_fulfilled
FROM {{ ref('fact_order_line_items') }} AS fct
GROUP BY
    fct.order_id,
    fct.customer_id,
    fct.order_date
ORDER BY
    fct.order_date,
    fct.order_id