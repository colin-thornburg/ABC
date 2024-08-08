{{ config(materialized='view') }}

SELECT
    user_id,
    bu_code,
    source_system,
    role_based_code,
    user_access_group_id
FROM {{ ref('user_access_group') }}