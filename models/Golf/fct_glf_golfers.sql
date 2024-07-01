{{ config(materialized='table') }}

WITH golfer_details AS (
    SELECT
        *
    FROM {{ ref('cur_glf_golfers') }} c
    
),

golfer_history AS (
    SELECT
        *
    FROM {{ ref('arc_glf_golfers') }} a
    -- Apply any necessary filters or transformations for historical data
)

-- Final select query that prepares the data for analysis
SELECT
    d.Golfer_ID,
    d.First_Name,
    d.Last_Name,
    d.Middle_Initial,
    d.Date_Of_Birth,
    d.Email_Address,
    
    -- Include any additional derived columns, metrics, or dimensions necessary for analysis
FROM golfer_details d
-- Consider how to include historical data from golfer_history if needed
