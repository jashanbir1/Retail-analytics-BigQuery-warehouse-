{{ config(
    materialized='table',
    schema='retail_gold'
) }}

SELECT 
    order_date,
    count(distinct order_id) as total_orders,
    sum(quantity) as total_units_sold,
    sum(line_revenue) as total_revenue

FROM {{ ref('fact_order_line_items')}}
GROUP BY order_date
ORDER BY order_date ASC
