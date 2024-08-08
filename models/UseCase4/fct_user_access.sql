{{ config(
    materialized='incremental',
    unique_key=['user_access_group_id', 'bu_key', 'source_system'],
    on_schema_change='sync_all_columns',
    post_hook="DELETE FROM {{ this }} t
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ ref('int_user_access_expanded') }} s
        WHERE s.user_access_group_id = t.user_access_group_id
        AND s.bu_key = t.bu_key
        AND s.source_system = t.source_system
    )"
) }}

-- macro above simply deletes records in the fct model that no longer exist in the source table

WITH source_data AS (
    SELECT * FROM {{ ref('int_user_access_expanded') }}
),
valid_data AS (
    SELECT *
    FROM source_data
    WHERE bu_key != 0  -- Exclude records with bu_key = 0
)
SELECT
    user_access_group_id,
    bu_key,
    source_system,
    user_id,
    role_based_code,
    access_type,
    current_timestamp() as etl_updated_at
FROM valid_data

{% if is_incremental() %}
WHERE etl_updated_at > (SELECT MAX(etl_updated_at) FROM {{ this }})
{% endif %}