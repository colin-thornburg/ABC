{% set source_system = var('source_system', 'EPIC_US') %}

with source as (
    select * from {{ ref('dim_epic_policy_line_type_' ~ source_system) }}
),

staged as (
    select
        '{{ source_system }}' as source_system,
        epic_policy_line_type_key,
        office_agency_system_key,
        policy_line_type_code,
        policy_line_type_desc
    from source
)

select * from staged