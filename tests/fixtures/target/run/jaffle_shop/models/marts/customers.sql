
  
  create view "jaffle_shop"."main"."customers__dbt_tmp" as (
    with customers as (
    select * from "jaffle_shop"."main"."stg_customers"
),

orders as (
    select * from "jaffle_shop"."main"."stg_orders"
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
  );
