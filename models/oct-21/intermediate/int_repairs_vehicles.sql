{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
    r.repair_id,
    r.vehicle_id,
    r.repair_date,
    r.repair_type,
    r.repair_cost,
    v.vehicle_make,
    v.vehicle_model,
    v.vehicle_year,
    v.owner_id
FROM {{ ref('stg_repairs') }} r
JOIN {{ ref('stg_vehicles') }} v ON r.vehicle_id = v.vehicle_id