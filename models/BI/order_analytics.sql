with
customers as (select * from {{ ref("dim_customers") }}),

orders as (select * from {{ ref("fct_orders") }})

select
    orders.order_id,
    orders.customer_id,
    customers.first_name,
    customers.last_name,
    orders.order_date,
    orders.state as state,
    orders.payment_id,
    orders.payment_method,
    {{ format_dollars('amount') }} as amount,
    orders.created_at,
    customers.first_order_date,
    customers.most_recent_order_date
from orders
left join customers on orders.customer_id = customers.customer_id

