with temp_policy as (
    select * from {{ ref('int_temp_policy_multi_year_1') }}
),
current_year as (
    select year(current_timestamp()) as year
)

select 
    policy_key,
    multi_year_ind,
    case
        when effective_year between year - 1 and year then 1
        else 0
    end as cy_eff_factor,
    sum(case when effective_year between year - 1 and year or expiration_year = year then 1 else 0 end) over(partition by renewal_policy_id, source_system_instance_key, division_code) 
    - 
    (case when effective_year between year - 1 and year or expiration_year = year then 1 else 0 end) as cy_exp_factor,
    case
        when effective_year between year - 2 and year - 1 then 1
        else 0
    end as py_eff_factor,
    sum(case when effective_year between year - 2 and year - 1 or expiration_year = year - 1 then 1 else 0 end) over(partition by renewal_policy_id, source_system_instance_key, division_code) 
    - 
    (case when effective_year between year - 2 and year - 1 or expiration_year = year - 1 then 1 else 0 end) as py_exp_factor,
    min(case when effective_year between year - 1 and year or expiration_year = year then multi_year_ind else null end) over(partition by renewal_policy_id, source_system_instance_key, division_code) as cy_min_multiyear_ind,
    max(case when effective_year between year - 1 and year or expiration_year = year then multi_year_ind else null end) over(partition by renewal_policy_id, source_system_instance_key, division_code) as cy_max_multiyear_ind,
    min(case when effective_year between year - 2 and year - 1 or expiration_year = year - 1 then multi_year_ind else null end) over(partition by renewal_policy_id, source_system_instance_key, division_code) as py_min_multiyear_ind,
    max(case when effective_year between year - 2 and year - 1 or expiration_year = year - 1 then multi_year_ind else null end) over(partition by renewal_policy_id, source_system_instance_key, division_code) as py_max_multiyear_ind
from temp_policy, current_year