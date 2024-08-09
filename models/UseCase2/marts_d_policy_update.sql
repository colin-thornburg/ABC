{{ config(
    materialized='incremental',
    unique_key='policy_key'
) }}
-- merge_update_columns = ['current_year_multi_year_exclusion_ind', 'prior_year_multi_year_exclusion_ind'],


Select * from {{ ref('int_temp_policy_multi_year_final') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}



