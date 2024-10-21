{{
    config(
        materialized='view'
    )
}}

SELECT
    CAST(repair_id AS INTEGER) AS repair_id,
    CAST(vehicle_id AS VARCHAR(10)) AS vehicle_id,
    CAST(repair_date AS DATE) AS repair_date,
    CAST(repair_type AS VARCHAR(50)) AS repair_type,
    CAST(cost AS DECIMAL(10,2)) AS repair_cost
FROM {{ ref('repairs') }}