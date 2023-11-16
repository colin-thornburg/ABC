{{ config(materialized='view') }}

SELECT *
FROM {{ source('fdh_sap', 'dim_xxetn_map_unit_sap_ora') }}
