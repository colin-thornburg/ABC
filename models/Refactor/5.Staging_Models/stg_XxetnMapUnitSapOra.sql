With XxetnMapUnitSapOra AS (
    SELECT *
    FROM {{ source('fdh_slt_raw', 'dim_xxetn_map_unit_sap_ora') }}
    WHERE ifnull(sap_company_code, '') <> ''
      AND active_flag = 'Y'
)

Select * from XxetnMapUnitSapOra