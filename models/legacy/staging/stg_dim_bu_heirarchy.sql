{{ config(materialized='view') }}

SELECT *
FROM {{ source('fdh_sap', 'dim_bu_heirarchy') }}
