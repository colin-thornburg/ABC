{{ config(
    materialized='incremental',
    unique_key=['division_code', 'item_code'],
    strategy='merge'
) }}

/***
 This incremental model for hub_item offers several advantages over the initial script:
-- 1. Efficiency: It only processes new or changed data since the last run, reducing processing time and resource usage.
-- 2. Simplicity: The logic is straightforward, making it easier to understand and maintain.
-- 3. Data Integrity: It ensures only one record per unique item, maintaining the core principle of a hub table.
-- 4. Historization: While not tracking full history (that's for satellites), it does track last update time.

Benefits
-- - No need for complex windowing functions or subqueries for deduplication
-- - Processes less data on each run due to incremental logic
***/

WITH source_data AS (
    SELECT 
        division_code,
        item_code,
        department,
        item_group,
        subgroup,
        gpc_merchandise_class_code,
        cdc_timestamp
    FROM {{ ref('int_item_transformed') }}
    WHERE header__operation != 'BEFOREIMAGE'  -- Exclude BEFOREIMAGE records
    {% if is_incremental() %}
    AND cdc_timestamp > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['division_code', 'item_code']) }} AS item_key,
    division_code,
    item_code,
    department,
    item_group,
    subgroup,
    gpc_merchandise_class_code,
    CURRENT_TIMESTAMP() AS created_at,
    MAX(cdc_timestamp) AS updated_at
FROM source_data

-- ensures only one record per unique item, even if there are multiple updates in a single run
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8