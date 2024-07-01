with
customers as (select * from {{ ref("dim_customers") }}),

orders as (select * from {{ ref("fct_orders") }})

select
    orders.order_id
from orders
left join customers on orders.customer_id = customers.customer_id
