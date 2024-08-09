with multi_year_data as (
    select * from {{ ref('int_temp_policy_multi_year_2') }}
)

select 
    policy_key,
    case
        when cy_eff_factor = 1 and cy_exp_factor >= 1 and not cy_min_multiyear_ind and cy_max_multiyear_ind then true
        else multi_year_ind
    end as current_year_multi_year_exclusion_ind,
    case
        when py_eff_factor = 1 and py_exp_factor >= 1 and not py_min_multiyear_ind and py_max_multiyear_ind then true
        else multi_year_ind
    end as prior_year_multi_year_exclusion_ind
from multi_year_data