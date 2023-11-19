-- intermediate_accounting_document_aggregation

{{ config(materialized='view') }}

with accounting_document as (
    select
        fadh.le_number as legal_entity,
        fadh.document_number,
        fadh.fiscal_year,
        fadh.invoice_type,
        fadh.src_system_name_s,
        fadh.slt_delete_s,
        fadli.account_type,
        fadli.general_ledger_account,
        fadli.debit_credit_ind,
        fadli.lc2_base_amount,
        fadli.lc_amount,
        fadli.line_item
    from {{ ref('staging_fact_accounting_document_header') }} fadh
    inner join {{ ref('staging_fact_accounting_document_line_items') }} fadli
        on fadh.le_number = fadli.le_number
        and fadh.document_number = fadli.document_number
        and fadh.fiscal_year = fadli.fiscal_year
    where
        ifnull(fadli.slt_delete_s, '') <> 'X'
        -- Additional filtering can be applied here based on your requirements
),

-- Additional aggregations or transformations can be added here

select
    ad.legal_entity,
    ad.document_number,
    ad.fiscal_year,
    ad.invoice_type,
    -- Additional columns and aggregations as required
from accounting_document ad
group by
    ad.legal_entity,
    ad.document_number,
    ad.fiscal_year,
    ad.invoice_type
    -- Additional grouping columns as required
