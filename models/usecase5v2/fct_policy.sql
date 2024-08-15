{{
    config(
        materialized='incremental',
        unique_key='policy_key'
    )
}}

Select * from {{ ref('int_policy_epic_us_final') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where effective_date > (select max(effective_date) from {{ this }}) 
{% endif %}