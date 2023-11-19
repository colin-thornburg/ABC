-- intermediate_customer_mapping

{{ config(materialized='view') }}

select
    dc.party_number_s as customer_id,
    dc.le_number_s as customer_legal_entity,
    dc.src_system_name_s as source_system,
    xx.sap_company_code,
    xx.sap_profit_center,
    xx.site,
    xx.country,
    xx.region,
    xx.ar_credit_office,
    xx.support_center,
    bh.ledger as bu_hierarchy_ledger,
    bh.active_flag as bu_hierarchy_active
from {{ ref('staging_dim_customer') }} dc
left join {{ ref('staging_xxetn_map_unit_sap_ora') }} xx
    on dc.le_number_s = xx.le_number
    and dc.party_number_s = xx.sap_company_code
left join {{ ref('staging_dim_bu_hierarchy') }} bh
    on xx.site = bh.ledger
    and bh.active_flag = 'Y'