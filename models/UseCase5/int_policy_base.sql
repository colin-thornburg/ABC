{% set source_system = var('source_system', 'EPIC_US') %}

with policy_base as (
    select
        p.source_system,
        p.policy_key,
        p.extract_key,
        p.office_agency_system_key,
        p.client_key,
        p.insurer_market_key,
        p.payee_market_key,
        p.product_line_key,
        p.producer1_employee_key,
        p.producer2_employee_key,
        p.csr1_employee_key,
        p.department_key,
        p.policy_num,
        p.policy_status,
        p.effective_date,
        p.expiration_date,
        p.inception_date,
        p.estimated_premium_amt,
        p.epic_policy_type_key,
        p.annualized_endorsement_premium_amt,
        p.written_premium_amt,
        p.annualized_premium_amt,
        p.contracted_expiration_date,
        p.bill_type_key,
        e.agency_system_name,
        e.agency_system_name || ' - ' || e.office_agency_system_key as source_system_instance_code
    from {{ ref('stg_dim_policy') }} p
    inner join {{ ref('stg_dim_extract') }} e 
        on p.extract_key = e.extract_key
        and p.source_system = e.source_system
    where p.office_agency_system_key = 
        {% if source_system == 'EPIC_US' %}
            1
        {% elif source_system == 'EPIC_CAN' %}
            2
        {% else %}
            -1
        {% endif %}
    qualify row_number() over (
        partition by p.source_system, p.policy_num, p.office_agency_system_key
        order by p.extract_key desc, p.policy_key desc
    ) = 1
)

select * from policy_base