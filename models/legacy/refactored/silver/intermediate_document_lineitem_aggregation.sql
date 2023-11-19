-- intermediate_document_lineitem_aggregation.sql
{{ config(materialized='view') }}

with lineitem as (
    select
        li.legal_entity,
        li.document_number,
        li.fiscal_year,
        li.billing_doc,
        li.src_system_name,
        adh.invoice_type,
        dpt.min_day_limit_s,
        dpt.max_day_limit_s
        -- You can include additional payment terms related fields as necessary
    from {{ ref('staging_fact_ar_invoice_lineitem') }} li
    inner join {{ ref('staging_fact_accounting_document_header') }} adh
        on li.legal_entity = adh.le_number
        and li.document_number = adh.document_number
        and li.fiscal_year = adh.fiscal_year
    left join {{ ref('staging_dim_payment_terms') }} dpt
        on li.pay_term = dpt.term_id_s
    where
        ifnull(li.slt_delete, '') <> 'X'
        and li.src_system_name = 'sap'
        and adh.invoice_type not in ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')
        and ifnull(dpt.slt_delete, '') <> 'X'
        and dpt.active_flag_s = 'Y'
    group by
        li.legal_entity,
        li.document_number,
        li.fiscal_year,
        li.billing_doc,
        li.src_system_name,
        adh.invoice_type,
        dpt.min_day_limit_s,
        dpt.max_day_limit_s
)

select * from lineitem
