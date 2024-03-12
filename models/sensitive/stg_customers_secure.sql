{{
    config(
        materialized='view',
        secure='true'
    )
}}

select 
    id as customer_id,
    case 
        when current_role() = 'TRANSFORMER' then 'XXXX' -- Masking for transformer role
        else first_name
    end as first_name,
    case 
        when current_role() = 'TRANSFORMER' then 'XXXX' -- Masking for transformer role
        else last_name
    end as last_name,
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name']) }} as surr_col
from {{ source('jaffle_shop', 'customers') }}

