{{
    config(
        materialized = 'table'
    )
}}

select *,(depth*porosity) as depth_poro_product
from
{{ref("sample2")}}