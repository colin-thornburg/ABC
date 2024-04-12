-- models/hub_load_dim_product.sql
{{ config(
    materialized='incremental',
    unique_key='item_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        item_id,
        item_num,
        description,
        load_date,
        category_name,
        category_description
    FROM {{ ref('int_imaster_category') }}
)

SELECT
    s.item_id,
    s.item_num,
    s.description,
    s.load_date,
    s.category_name,
    s.category_description
FROM source_data s

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  where s.load_date >= (select max(load_date) from {{ this }})

{% endif %}