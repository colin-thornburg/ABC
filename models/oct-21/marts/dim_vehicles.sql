{{
    config(
        materialized='incremental',
        unique_key='vehicle_id'
    )
}}

SELECT
    vehicle_id,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    owner_id
FROM {{ ref('stg_vehicles') }}

{% if is_incremental() %}
    WHERE vehicle_id NOT IN (SELECT vehicle_id FROM {{ this }})
{% endif %}