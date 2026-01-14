-- noqa: disable=LT05,LT14
-- NOTE: CTE test generator skips CTEs inside comments when finding matches,
-- allowing you to keep old/alternate versions during refactoring.
-- Supports: SQL comments (--), block comments (/* */), Jinja comments ({# #})
with customers as (
    select * from {{ ref('stg_customers') }}
),

{% if true %}
    {# No-op Jinja block to test generator handles wrapped CTEs #}
    orders as (
        select * from {{ ref('stg_orders') }}
    ),
{% endif %}

orders_with_flags as (
    select
        order_id,
        customer_id,
        order_date,
        status,
        case when lower(status) = 'completed' then 1 else 0 end as is_completed,
        case when lower(status) in ('cancelled', 'canceled', 'returned', 'refunded') then 1 else 0 end as is_cancelled_or_returned
    from orders
),

order_rankings as (
    select
        *,
        row_number() over (partition by customer_id order by order_date desc, order_id desc) as rn_desc,
        row_number() over (partition by customer_id order by order_date asc, order_id asc) as rn_asc
    from orders_with_flags
),

-- Old version of customer_agg (commented out during refactoring)
-- customer_agg as (
--     select customer_id, count(*) as order_count
--     from orders_with_flags
--     group by customer_id
-- ),

/* Another old version with block comment
customer_agg as (
    select customer_id, min(order_date) as first_date
    from orders_with_flags
    group by customer_id
),
*/

{# Jinja-commented version during testing
customer_agg as (
    select customer_id, count(distinct order_id) as order_count
    from orders_with_flags
    group by customer_id
),
#}

customer_agg as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(is_completed) as number_of_completed_orders,  -- total completed (including refunds)
        sum(is_cancelled_or_returned) as number_of_cancelled_orders  /* cancelled/returned count ) */
    from orders_with_flags
    group by customer_id
),

recent_completed as (
    select
        customer_id,
        order_date as most_recent_completed_order_date
    from order_rankings
    where is_completed = 1 and rn_desc = 1
),

first_completed as (
    select
        customer_id,
        order_date as first_completed_order_date
    from order_rankings
    where is_completed = 1 and rn_asc = 1
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_agg.first_order_date,
        customer_agg.most_recent_order_date,
        recent_completed.most_recent_completed_order_date,
        first_completed.first_completed_order_date,
        coalesce(customer_agg.number_of_orders, 0) as number_of_orders,
        coalesce(customer_agg.number_of_completed_orders, 0) as number_of_completed_orders,
        coalesce(customer_agg.number_of_cancelled_orders, 0) as number_of_cancelled_orders
    from customers
    left join customer_agg on customers.customer_id = customer_agg.customer_id
    left join recent_completed on customers.customer_id = recent_completed.customer_id
    left join first_completed on customers.customer_id = first_completed.customer_id
)

select * from final
