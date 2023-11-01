{{
    config(
        materialized='incremental',
        unique_key='person_id'
    )
}}

-- changed records

Select * From {{ ref('sales_data') }}
{% if is_incremental() %}

    Where sale_date > (Select max(sale_date) From {{ this }})

{% endif %}
