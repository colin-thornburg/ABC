{{
    config(
        materialized='table'
    )
}}

select 
    id as customer_id,
    first_name,
    last_name,
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name']) }} as surr_col
from {{ source('jaffle_shop', 'customers') }}