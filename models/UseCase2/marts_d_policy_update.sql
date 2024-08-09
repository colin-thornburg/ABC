{{ config(
    materialized='incremental',
    unique_key='policy_key'
) }}
-- merge_update_columns = ['current_year_multi_year_exclusion_ind', 'prior_year_multi_year_exclusion_ind'],
with final_data as (
    select 
        policy_key,
        current_year_multi_year_exclusion_ind,
        prior_year_multi_year_exclusion_ind
    from {{ ref('int_temp_policy_multi_year_final') }}
)

 edupdatew.d_policy as target
set
    target.current_year_multi_year_exclusion_ind = source.current_year_multi_year_exclusion_ind,
    target.prior_year_multi_year_exclusion_ind = source.prior_year_multi_year_exclusion_ind,
    target.etl_process_run_id = '{{ etl_process_run_id }}',
    target.etl_process_name = '{{ etl_process_name }}',
    target.etl_update_datetime = current_timestamp()
from final_data as source
where source.policy_key = target.policy_key
and (
    ifnull(cast(source.current_year_multi_year_exclusion_ind as int), 9) <> ifnull(cast(target.current_year_multi_year_exclusion_ind as int), 9)
    or ifnull(cast(source.prior_year_multi_year_exclusion_ind as int), 9) <> ifnull(cast(target.prior_year_multi_year_exclusion_ind as int), 9)
);