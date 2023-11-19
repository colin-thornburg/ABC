-- stg_fdh_slt_raw_dim_bu_heirarchy.sql
select * from {{ source('fdh_slt_raw', 'dim_bu_heirarchy') }}
