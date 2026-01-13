-- Build actual result given inputs
with dbt_internal_unit_test_actual as (
  select
    "customer_id","first_name","last_name","first_order_date","most_recent_order_date","number_of_orders", 'actual' as "actual_or_expected"
  from (
    with  __dbt__cte__stg_customers as (

-- Fixture for stg_customers
select 
    
    cast(50 as INTEGER)
 as "customer_id", 
    
    cast('Emma' as character varying(256))
 as "first_name", 
    
    cast('Wilson' as character varying(256))
 as "last_name"
),  __dbt__cte__stg_orders as (

-- Fixture for stg_orders
select 
    
    cast(100 as INTEGER)
 as "order_id", 
    
    cast(50 as INTEGER)
 as "customer_id", 
    
    cast('2024-01-15' as DATE)
 as "order_date", 
    
    cast('completed' as character varying(256))
 as "status"
), customers as (
    select * from __dbt__cte__stg_customers
),

orders as (
    select * from __dbt__cte__stg_orders
),

-- TESTING: Step 1 - before parse, run, parse sequence
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
    from customers
    left join
        customer_orders
        on customers.customer_id = customer_orders.customer_id
)

select * from final
  ) _dbt_internal_unit_test_actual
),
-- Build expected result
dbt_internal_unit_test_expected as (
  select
    "customer_id", "first_name", "last_name", "first_order_date", "most_recent_order_date", "number_of_orders", 'expected' as "actual_or_expected"
  from (
    select 
    
    cast(50 as INTEGER)
 as "customer_id", 
    
    cast('Emma' as character varying(256))
 as "first_name", 
    
    cast('Wilson' as character varying(256))
 as "last_name", 
    
    cast('2024-01-15' as DATE)
 as "first_order_date", 
    
    cast('2024-01-15' as DATE)
 as "most_recent_order_date", 
    
    cast(1 as BIGINT)
 as "number_of_orders"
  ) _dbt_internal_unit_test_expected
)
-- Union actual and expected results
select * from dbt_internal_unit_test_actual
union all
select * from dbt_internal_unit_test_expected