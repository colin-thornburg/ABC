{{ config(materialized='table') }}

WITH golfer_details AS (
    SELECT
        c.Golfer_ID,
        c.First_Name,
        c.Last_Name,
        c.Middle_Initial,
        c.Date_Of_Birth,
        c.Email_Address,
        c.LOAD_KEY_HASH,
        c.JOB_EXECUTION_ID,
        c.CREATE_TIMESTAMP,
        c.LOAD_TIMESTAMP
    FROM {{ ref('cur_glf_golfers') }} c
    
),

golfer_history AS (
    SELECT
        a.Golfer_ID,
        a.First_Name,
        a.Last_Name,
        a.Middle_Initial,
        a.Date_Of_Birth,
        a.Email_Address,
        a.DML_TYPE_CODE,
        a.AUD_TYPE,
        a.LOAD_KEY_HASH,
        a.JOB_EXECUTION_ID,
        a.CREATE_TIMESTAMP,
        a.LOAD_TIMESTAMP
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
    d.CREATE_TIMESTAMP,
    -- Include any additional derived columns, metrics, or dimensions necessary for analysis
FROM golfer_details d
-- Consider how to include historical data from golfer_history if needed
