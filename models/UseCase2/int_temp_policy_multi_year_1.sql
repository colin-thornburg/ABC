with policy_data as (
    select 
        a.policy_key, 
        c.policy_id AS renewal_policy_id, 
        a.source_system_instance_key,
        d.division_code,
        b.multi_year_ind,
        year(b.effective_date) AS effective_year,
        year(b.expiration_date) AS expiration_year
    from {{ ref('stg_edw_f_policy') }} as a
    inner join {{ ref('stg_edw_d_policy') }} as b on a.policy_key = b.policy_key
    inner join {{ ref('stg_edw_d_policy') }} as c on a.renewal_policy_key = c.policy_key
    inner join {{ ref('stg_edw_d_bu') }} as d on a.bu_key = d.bu_key
    where a.env_source_code = '{{ env_source_code }}'
    and a.renewal_policy_key > {{ env_variables['unknown_key'] }}
)
select * 
from policy_data