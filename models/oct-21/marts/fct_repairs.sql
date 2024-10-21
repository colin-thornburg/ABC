{{
    config(
        materialized='incremental',
        unique_key='repair_id'
    )
}}

SELECT
    r.repair_id,
    r.vehicle_id,
    r.repair_date,
    r.repair_type,
    r.repair_cost,
    o.owner_id,
    o.first_name AS owner_first_name,
    o.last_name AS owner_last_name
FROM {{ ref('int_repairs_vehicles') }} r
JOIN {{ ref('stg_owners') }} o ON r.owner_id = o.owner_id

{% if is_incremental() %}
    WHERE r.repair_date > (SELECT MAX(repair_date) FROM {{ this }})
{% endif %}