-- Modified int_temp_policy_multi_year_1 model
-- Addressing undefined variables and data type issues

with policy_data as (
    select 
        a.policy_key, 
        c.policy_id AS renewal_policy_id, 
        a.source_system_instance_key,
        d.division_code,
        b.multi_year_ind,
        -- Cast to DATE before extracting year
        year(try_cast(b.effective_date as date)) AS effective_year,
        year(try_cast(b.expiration_date as date)) AS expiration_year
    from {{ ref('stg_edw_f_policy') }} as a
    inner join {{ ref('stg_edw_d_policy') }} as b on a.policy_key = b.policy_key
    inner join {{ ref('stg_edw_d_policy') }} as c on a.renewal_policy_key = c.policy_key
    inner join {{ ref('stg_edw_d_bu') }} as d on a.bu_key = d.bu_key
    where 1=1
        -- Original: a.env_source_code = '{{ env_source_code }}'
        -- Replace 'PROD' with the appropriate environment code
        and a.env_source_code = 'PROD'
        
        -- Original: a.renewal_policy_key > ''' env_variables['unknown_key'] }}
        -- Replace 0 with an appropriate minimum value for renewal_policy_key
        and a.renewal_policy_key > 0
)
select * 
from policy_data