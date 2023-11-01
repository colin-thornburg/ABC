{{
    config(
        materialized='view'
    )
}}

-- Select all records from the cold_data model
SELECT * FROM {{ ref('cold_data') }}

-- Union with all records from the hot_data model
UNION ALL

SELECT * FROM {{ ref('hot_data') }}