{{
    config(
        materialized='view'
    )
}}

SELECT
    CAST(owner_id AS VARCHAR(10)) AS owner_id,
    CAST(first_name AS VARCHAR(50)) AS first_name,
    CAST(last_name AS VARCHAR(50)) AS last_name,
    CAST(email AS VARCHAR(100)) AS email
FROM {{ ref('owners') }}