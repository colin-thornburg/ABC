-- models/staging/stg_dim_policy.sql

{% set source_system = var('source_system', 'EPIC_US') %}

with source as (
    select * from {{ ref('dim_policy_' ~ source_system) }}
),

staged as (
    select
        '{{ source_system }}' as source_system,
        policy_key,
        extract_key,
        office_agency_system_key,
        client_key,
        broker_key,
        insurer_market_key,
        payee_market_key,
        product_line_key,
        producer1_employee_key,
        producer2_employee_key,
        csr1_employee_key,
        department_key,
        policy_number as policy_num,
        policy_status,
        effective_date,
        expiration_date,
        inception_date,
        estimated_premium as estimated_premium_amt,
        epic_policy_type_key,
        annualized_endorsement_premium as annualized_endorsement_premium_amt,
        written_premium as written_premium_amt,
        annualized_premium as annualized_premium_amt,
        contracted_expiration_date,
        bill_type_key
    from source
)

select * from staged