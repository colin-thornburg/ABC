--{{ ref('stg_source1') }}
--{{ ref('stg_source2') }}


{{
    config(
        materialized='incremental',
        unique_key='id',
        merge_update_columns=var('column_to_update'),
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

SELECT
    id,
    column1,
    column2,
    updated_at
FROM {{ ref(var('my_source')) }}
{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}