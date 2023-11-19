-- staging_xxetn_map_unit_sap_ora

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('fdh_sap', 'dim_xxetn_map_unit_sap_ora') }}
    where active_flag = 'Y'
    and ifnull(sap_company_code, '') <> ''
)

select * from source_data
