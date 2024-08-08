{{ config(materialized='table') }}

WITH user_access AS (
    SELECT * FROM {{ ref('stg_user_access') }}
),
bu_expansion AS (
    SELECT * FROM {{ ref('bu_expansion') }}
)
SELECT
    ua.user_id,
    be.bu_key,
    ua.source_system,
    ua.role_based_code,
    ua.user_access_group_id,
    CASE
        WHEN ua.role_based_code = 'BU' THEN 'BU'
        WHEN ua.role_based_code = 'SourceSystem' THEN 'SourceSystem'
        ELSE 'Unknown'
    END AS access_type
FROM user_access ua
LEFT JOIN bu_expansion be ON ua.bu_code = be.bu_code