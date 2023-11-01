{{
    config(
        materialized='incremental',
        unique_key='person_id',
        post_hook=[
            "TRUNCATE TABLE {{ ref('hot_data') }}"
        ]
    )
}}

SELECT *
FROM {{ ref('hot_data') }}


-- During incremental runs, pull only the new records from the hot_data table.
{% if is_incremental() %}

    WHERE person_id NOT IN (SELECT person_id FROM {{ this }})

{% endif %}
