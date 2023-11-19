-- stg_fdh_slt_raw_dim_xxetn_map_unit_sap_ora.sql
select * from {{ source('fdh_slt_raw', 'dim_xxetn_map_unit_sap_ora') }}
