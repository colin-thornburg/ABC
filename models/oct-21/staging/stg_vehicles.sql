{{
    config(
        materialized='view'
    )
}}

SELECT
    CAST(vehicle_id AS VARCHAR(10)) AS vehicle_id,
    CAST(make AS VARCHAR(50)) AS vehicle_make,
    CAST(model AS VARCHAR(50)) AS vehicle_model,
    CAST(year AS INTEGER) AS vehicle_year,
    CAST(owner_id AS VARCHAR(10)) AS owner_id
FROM {{ ref('vehicles') }}