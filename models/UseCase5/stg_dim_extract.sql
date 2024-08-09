{% set source_system = var('source_system', 'EPIC_US') %}

with source as (
    select * from {{ ref('dim_extract_' ~ source_system) }}
),

staged as (
    select
        '{{ source_system }}' as source_system,
        extract_key,
        office_agency_system_key,
        agency_system_name
    from source
)

select * from staged