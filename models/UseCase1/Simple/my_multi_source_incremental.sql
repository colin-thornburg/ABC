{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}


WITH model1 AS (
    SELECT
        id,
        column1,
        updated_at,
        NULL AS column2
    FROM {{ ref('my_source_1') }}
    {% if is_incremental() %}
        WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
    {% endif %}
),
model2 AS (
    SELECT
        id,
        NULL AS column1,
        column2,
        updated_at
    FROM {{ ref('my_source_2') }}
    {% if is_incremental() %}
        WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
    {% endif %}
),
combined AS (
    SELECT
        COALESCE(m1.id, m2.id) AS id,
        COALESCE(m1.column1, t.column1) AS column1,
        COALESCE(m2.column2, t.column2) AS column2,
        GREATEST(COALESCE(m1.updated_at, '1900-01-01'), COALESCE(m2.updated_at, '1900-01-01')) AS updated_at
    FROM model1 m1
    FULL OUTER JOIN model2 m2 ON m1.id = m2.id
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t ON COALESCE(m1.id, m2.id) = t.id
    {% else %}
    LEFT JOIN (SELECT NULL AS id, NULL AS column1, NULL AS column2, NULL AS updated_at) t ON 1=0
    {% endif %}
)

SELECT * FROM combined