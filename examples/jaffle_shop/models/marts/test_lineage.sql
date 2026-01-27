{{ config(
    materialized='view',
    tags=['test', 'lineage']
) }}

/*
Test model for column lineage with complex transformations.
Tests: CTEs, UNION ALL, aggregations, window functions, CASE, COALESCE, joins.
*/

with

-- CTE 1: Simple SELECT * (wildcard pattern)
raw_orders as (
    select * from {{ ref('stg_orders') }}
),

-- CTE 2: Column transformations with expressions
enriched_orders as (
    select
        order_id,
        customer_id,
        order_date,
        status,
        -- CASE expression
        case
            when status = 'completed' then 1
            when status = 'pending' then 0
            else -1
        end as status_code,
        -- Date transformation
        date_trunc('month', order_date) as order_month
    from raw_orders
),

-- CTE 3: Aggregation with GROUP BY
customer_stats as (
    select
        customer_id,
        count(order_id) as total_orders,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(case when status = 'completed' then 1 end) as completed_orders,
        sum(status_code) as total_status_score
    from enriched_orders
    group by customer_id
),

-- CTE 4: Window function
orders_with_rank as (
    select
        order_id,
        customer_id,
        order_date,
        row_number()
            over (
                partition by customer_id
                order by order_date
            )
        as order_sequence,
        lag(order_date)
            over (
                partition by customer_id
                order by order_date
            )
        as previous_order_date
    from enriched_orders
),

-- CTE 5: Complex join with multiple CTEs
customer_enriched as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        cs.total_orders,
        cs.first_order_date,
        cs.last_order_date,
        c.first_name || ' ' || c.last_name as full_name,
        -- COALESCE expression
        coalesce(cs.completed_orders, 0) as completed_orders,
        -- Date calculation
        datediff(
            'day', cs.first_order_date, cs.last_order_date
        ) as customer_lifetime_days
    from {{ ref('stg_customers') }} as c
    left join customer_stats as cs on c.customer_id = cs.customer_id
),

-- CTE 6: Nested subquery
high_value_customers as (
    select
        customer_id,
        full_name,
        total_orders,
        completed_orders,
        completed_orders * 1.0 / nullif(total_orders, 0) as completion_rate
    from customer_enriched
    where total_orders >= 3
),

-- CTE 7: UNION branch 1 - Dynamic data
active_customers as (
    select
        customer_id,
        full_name,
        total_orders,
        completed_orders,
        completion_rate,
        customer_lifetime_days,
        'active' as customer_segment
    from high_value_customers
    where completion_rate > 0.5
),

-- CTE 8: UNION branch 2 - Dynamic with different logic
new_customers as (
    select
        customer_id,
        full_name,
        total_orders,
        completed_orders,
        0.0 as completion_rate,  -- Different from active
        customer_lifetime_days,
        'new' as customer_segment
    from customer_enriched
    where total_orders = 1
),

-- CTE 9: Combine dynamic branches
all_segments_union as (
    select * from active_customers
    union all
    select * from new_customers
),

-- CTE 9.5: Combine dynamic branches
all_segments as (
    select * from all_segments_union
    union all
    select * from all_segments_union

),

-- CTE 10: Final transformation with complex expression
final as (
    select
        seg.customer_id,
        seg.full_name,
        seg.customer_segment,
        seg.total_orders,
        seg.completed_orders,
        seg.completion_rate,
        seg.customer_lifetime_days,
        -- Complex nested expression
        owr.order_sequence as latest_order_number,
        -- Window function across segments
        owr.previous_order_date,
        -- Combination with latest order
        case
            when seg.completion_rate >= 0.8 then 'excellent'
            when seg.completion_rate >= 0.5 then 'good'
            when seg.completion_rate >= 0.3 then 'fair'
            else 'poor'
        end as performance_tier,
        rank()
            over (
                partition by seg.customer_segment
                order by seg.total_orders desc
            )
        as segment_rank
    from all_segments as seg
    left join orders_with_rank as owr
        on
            seg.customer_id = owr.customer_id
            and owr.order_sequence = (
                select max(owr2.order_sequence)
                from orders_with_rank as owr2
                where owr2.customer_id = seg.customer_id
            )
)

-- Main query with UNION ALL including static default row
select
    customer_id,
    full_name,
    customer_segment,
    total_orders,
    completed_orders,
    completion_rate,
    performance_tier,
    segment_rank,
    customer_lifetime_days,
    latest_order_number,
    previous_order_date
from final

union all

-- Static "unknown" dimension row (common warehouse pattern)
select
    '-1' as customer_id,
    'Unknown' as full_name,
    'unknown' as customer_segment,
    0 as total_orders,
    0 as completed_orders,
    0.0 as completion_rate,
    'n/a' as performance_tier,
    0 as segment_rank,
    0 as customer_lifetime_days,
    0 as latest_order_number,
    null as previous_order_date
