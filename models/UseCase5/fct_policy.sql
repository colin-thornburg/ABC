{% set source_system = var('source_system', 'EPIC_US') %}

with policy_main as (
    select
        pb.policy_key,
        pb.client_key,
        pb.insurer_market_key as carrier_insurer_key,
        pb.payee_market_key as carrier_payee_key,
        eplt.epic_policy_line_type_key as product_key,
        pb.product_line_key,
        pb.producer1_employee_key as producer_01_key,
        pb.producer2_employee_key as producer_02_key,
        pb.csr1_employee_key as account_manager_key,
        {{ dbt_utils.date_trunc('day', 'pb.effective_date') }} as effective_date_key,
        {{ dbt_utils.date_trunc('day', 'pb.expiration_date') }} as expiration_date_key,
        {{ dbt_utils.date_trunc('day', 'pb.inception_date') }} as inception_date_key,
        {{ dbt_utils.date_trunc('day', 'pb.contracted_expiration_date') }} as contracted_expiration_date_key,
        coalesce(bu.bu_key, {{ var('unknown_key') }}) as bu_key,
        coalesce(bu_dept.bu_key, bu_dept_fdw.bu_key, {{ var('invalid_key') }}) as bu_department_key,
        pb.bill_type_key,
        pb.policy_num as policy_id,
        pb.estimated_premium_amt,
        pb.annualized_endorsement_premium_amt,
        pb.written_premium_amt,
        pb.annualized_premium_amt,
        pr.billed_premium_amt_lcl,
        pr.billed_premium_amt_usd,
        pr.commission_revenue_amt_lcl,
        pr.commission_revenue_amt_usd,
        pr.fee_revenue_amt_lcl,
        pr.fee_revenue_amt_usd
        -- Add other fields from policy_base and policy_revenue_detail
    from {{ ref('int_policy_base') }} pb
    left join {{ ref('stg_dim_epic_policy_line_type') }} eplt
        on pb.epic_policy_type_key = eplt.epic_policy_line_type_key
        and pb.office_agency_system_key = eplt.office_agency_system_key
        and pb.source_system = eplt.source_system
    left join {{ ref('int_policy_detail') }} pr
        on pb.policy_key = pr.policy_key
        and pb.source_system = pr.source_system
    where pb.source_system = '{{ source_system }}'
)

select
    policy_key,
    client_key,
    carrier_insurer_key,
    carrier_payee_key,
    product_key,
    product_line_key,
    producer_01_key,
    producer_02_key,
    account_manager_key,
    effective_date_key,
    expiration_date_key,
    inception_date_key,
    contracted_expiration_date_key,
    client_producer_key,
    client_account_manager_key,
    bu_key,
    bu_department_key,
    bill_type_key,
    policy_id,
    estimated_premium_amt,
    annualized_endorsement_premium_amt,
    written_premium_amt,
    annualized_premium_amt,
    billed_premium_amt_lcl,
    billed_premium_amt_usd,
    commission_revenue_amt_lcl,
    commission_revenue_amt_usd,
    fee_revenue_amt_lcl,
    fee_revenue_amt_usd
    -- Add other fields as needed
from policy_main