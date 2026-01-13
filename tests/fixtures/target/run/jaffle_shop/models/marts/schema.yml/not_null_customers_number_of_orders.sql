
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select number_of_orders
from "jaffle_shop"."main"."customers"
where number_of_orders is null



  
  
      
    ) dbt_internal_test