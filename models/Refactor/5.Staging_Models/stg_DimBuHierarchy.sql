With DimBuHierarchy AS (
    SELECT *
    FROM {{ source('fdh_slt_raw', 'dim_bu_heirarchy') }}
    WHERE active_flag = 'Y'
)

Select * from DimBuHierarchy