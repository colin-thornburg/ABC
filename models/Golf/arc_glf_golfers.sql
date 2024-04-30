{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (
    SELECT
        *,
        CURRENT_TIMESTAMP() AS effective_from  -- Capture when the record was processed into the archive.
    FROM {{ ref('stg_lnd_glf_golfers') }}
),

-- Select only records that are newer than the latest in the archive, for incremental runs.
new_records AS (
    SELECT
        sd.*
    FROM source_data sd
    {% if is_incremental() %}
    -- Apply this filter to include only records with a LOAD_TIMESTAMP greater than any currently in arc_glf_golfers.
    WHERE sd.LOAD_TIMESTAMP > (SELECT MAX(LOAD_TIMESTAMP) FROM {{ this }})
    {% endif %}
)

SELECT * FROM new_records