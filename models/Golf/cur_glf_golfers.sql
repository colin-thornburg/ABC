{{
    config(
        materialized='view'
    )
}}

WITH ranked_records AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY Golfer_ID
            ORDER BY effective_from DESC
        ) AS rn
    FROM {{ ref('arc_glf_golfers') }}
)

SELECT
    Golfer_ID,
    First_Name,
    Last_Name,
    Middle_Initial,
    Date_Of_Birth,
    Email_Address
    -- Exclude the 'rn' and any other non-required fields from the final select.
FROM ranked_records
WHERE rn = 1