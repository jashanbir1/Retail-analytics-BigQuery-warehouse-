{{ config(
    materialized='table',
    schema='retail_gold'
) }}

SELECT
    fct.product_id,
    dim.product_title,
    dim.vendor,
    count(distinct fct.order_id) as total_orders,
    sum(fct.quantity) as total_units_sold,
    sum(fct.line_revenue) as all_time_revenue,
    SAFE_DIVIDE(SUM(fct.line_revenue), COUNT(DISTINCT fct.order_id)) AS avg_revenue_per_order,
    SAFE_DIVIDE(SUM(fct.quantity), COUNT(DISTINCT fct.order_id)) AS avg_units_per_order

FROM {{ ref('fact_order_line_items')}} as fct
INNER JOIN {{ ref('dim_products')}} as dim
    on fct.product_id = dim.product_id
GROUP BY 
    fct.product_id,
    dim.product_title,
    dim.vendor
ORDER BY all_time_revenue DESC





