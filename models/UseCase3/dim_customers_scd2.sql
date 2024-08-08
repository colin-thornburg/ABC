{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'name', 'city'],
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT 
        customer_id,
        name,
        email,
        city,
        updated_at,
        '{{ var("source_system", "source1") }}' AS source_system
    FROM {{ ref('customers_source1') }}

    UNION ALL

    SELECT 
        customer_id,
        name,
        email,
        city,
        updated_at,
        '{{ var("source_system", "source2") }}' AS source_system
    FROM {{ ref('customers_source2') }}
),

ranked_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, name, city ORDER BY updated_at DESC) AS row_num
    FROM source_data
)

SELECT 
    customer_id,
    name,
    email,
    city,
    source_system,
    updated_at AS valid_from,
    LEAD(updated_at, 1, '9999-12-31') OVER (PARTITION BY customer_id ORDER BY updated_at) AS valid_to,
    row_num = 1 AS is_current,
    CASE 
        WHEN LAG(name) OVER (PARTITION BY customer_id ORDER BY updated_at) != name 
             OR LAG(city) OVER (PARTITION BY customer_id ORDER BY updated_at) != city
        THEN 1  -- Type 2 change
        WHEN LAG(email) OVER (PARTITION BY customer_id ORDER BY updated_at) != email
        THEN 2  -- Type 1 change
        ELSE 0 
    END AS change_type
FROM ranked_data
WHERE row_num = 1 OR
{% if is_incremental() %}
    updated_at > (SELECT MAX(valid_from) FROM {{ this }})
{% else %}
    1=1
{% endif %}