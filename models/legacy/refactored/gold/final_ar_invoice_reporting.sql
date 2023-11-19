-- final_ar_invoice_reporting

{{ config(materialized='view') }}

select
    um.*
from {{ ref('intermediate_union_model') }} um
where um.fiscal_year >= 2022
