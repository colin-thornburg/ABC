{{
    config(
        materialized='view',
        snowflake_warehouse='xsmall'
    )
}}


with
orders as (select * from {{ ref("stg_orders") }}),

payments as (select * from {{ ref("stg_payments") }})


select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.state,
    payments.payment_id,
    payments.payment_method,
    payments.amount,
    payments.created_at,
    payments.discount_percent


from orders
left join payments on orders.order_id = payments.order_id