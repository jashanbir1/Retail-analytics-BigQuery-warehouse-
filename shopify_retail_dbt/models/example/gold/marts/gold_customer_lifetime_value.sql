{{ config(
    materialized='table',
    schema='retail_gold'
) }}

SELECT
    fct.customer_id,
    dim.customer_full_name,
    dim.email,
    dim.phone_number,
    dim.currency,
    count(distinct fct.order_id) as total_orders,
    sum(fct.quantity) as total_units_purchased,
    sum(fct.line_revenue) as lifetime_revenue,
    SAFE_DIVIDE(SUM(fct.line_revenue), COUNT(DISTINCT fct.order_id)) AS average_order_value

FROM {{ ref('fact_order_line_items')}} as fct
INNER JOIN {{ ref('dim_customers')}} as dim
    on fct.customer_id = dim.customer_id
GROUP BY fct.customer_id, dim.customer_full_name, dim.email, dim.phone_number, dim.currency
ORDER BY lifetime_revenue DESC 