Select * from {{ ref('dim_customers') }}
where customer_id=1