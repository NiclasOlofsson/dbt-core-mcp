with  __dbt__cte__stg_customers as (

-- Fixture for stg_customers
select 
    
    cast(1 as INTEGER)
 as "customer_id", 
    
    cast('Alice' as character varying(256))
 as "first_name", 
    
    cast('Smith' as character varying(256))
 as "last_name"
union all
select 
    
    cast(2 as INTEGER)
 as "customer_id", 
    
    cast('Bob' as character varying(256))
 as "first_name", 
    
    cast('Jones' as character varying(256))
 as "last_name"
),  __dbt__cte__stg_orders as (

-- Fixture for stg_orders
select 
    
    cast(101 as INTEGER)
 as "order_id", 
    
    cast(1 as INTEGER)
 as "customer_id", 
    
    cast('2024-01-01' as DATE)
 as "order_date", cast(null as character varying(256)) as "status"
union all
select 
    
    cast(102 as INTEGER)
 as "order_id", 
    
    cast(1 as INTEGER)
 as "customer_id", 
    
    cast('2024-01-15' as DATE)
 as "order_date", cast(null as character varying(256)) as "status"
union all
select 
    
    cast(103 as INTEGER)
 as "order_id", 
    
    cast(1 as INTEGER)
 as "customer_id", 
    
    cast('2024-02-01' as DATE)
 as "order_date", cast(null as character varying(256)) as "status"
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