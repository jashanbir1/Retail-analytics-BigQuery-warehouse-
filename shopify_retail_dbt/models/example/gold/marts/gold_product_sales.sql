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
    sum(fct.line_revenue) as all_time_revenue

FROM {{ ref('fact_order_line_items')}} as fct
INNER JOIN {{ ref('dim_products')}} as dim
    on fct.product_id = dim.product_id
GROUP BY 
    fct.product_id,
    dim.product_title,
    dim.vendor
ORDER BY all_time_revenue DESC



