{{ config(materialized='view') }}

SELECT
{{ dbt_utils.generate_surrogate_key(['Golfer_ID', 'First_Name', 'Last_Name', 'Middle_Initial', 'Date_Of_Birth', 'Email_Address']) }} AS surrogate_key,
    NULLIF(TRIM(Golfer_ID), '')::INTEGER AS Golfer_ID,
    TRIM(First_Name) AS First_Name,
    TRIM(Last_Name) AS Last_Name,
    COALESCE(Middle_Initial, '') AS Middle_Initial,
    Date_Of_Birth,
    LOWER(Email_Address) AS Email_Address,
    LOAD_KEY_HASH,
    NULLIF(TRIM(JOB_EXECUTION_ID), '')::INTEGER AS JOB_EXECUTION_ID,
    CREATE_TIMESTAMP,
    LOAD_TIMESTAMP
FROM {{ ref('LND_GLF_Golfers') }}