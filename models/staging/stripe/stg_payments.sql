{{ config(
    tags=["finance"],
    query_tag = 'dbt_special'
) }}

select
    id as payment_id,
    order_id,
    payment_method,
    state,
    -- amount is stored in cents, convert it to dollars
    created as created_at,
    amount / 100 as amount,
    case
        when payment_method = 'coupon' then to_number(0.05, 10, 2)
        else to_number(0.00, 10, 2)
    end as discount_percent
from {{ source('stripe', 'payment') }}