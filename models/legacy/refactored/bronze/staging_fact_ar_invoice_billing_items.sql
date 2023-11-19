-- staging_fact_ar_invoice_billing_items

{{ config(materialized='view') }}

with source_data as (
    select
        select
            fact_ar_invoice_billing_items.billing_doc as billing_doc,
            fact_ar_invoice_lineitem.legal_entity,
            fact_ar_invoice_lineitem.customer,
            fact_ar_invoice_lineitem.indicator_spl_gl,
            fact_ar_invoice_lineitem.clearing_date,
            fact_ar_invoice_lineitem.receipt_number,
            fact_ar_invoice_lineitem.fiscal_year,
            fact_ar_invoice_lineitem.document_number,
            fact_ar_invoice_billing_items.billing_document_item
                as document_line_number,
            fact_ar_invoice_lineitem.posting_date,
            fact_ar_invoice_lineitem.document_date,
            fact_ar_invoice_lineitem.entered_on,
            fact_ar_invoice_lineitem.invoice_currency,
            fact_ar_invoice_lineitem.tax_code,
            fact_ar_invoice_lineitem.tax_code1 as tax_code1,
            fact_ar_invoice_billing_items.description as line_description,
            fact_ar_invoice_lineitem.gl_account,
            fact_ar_invoice_lineitem.gl_account_1,
            fact_ar_invoice_lineitem.baseline_date,
            fact_ar_invoice_lineitem.pay_term as pay_term,
            fact_ar_invoice_lineitem.invoice_ref,
            fact_ar_invoice_lineitem.trading_partner,
            fact_ar_invoice_lineitem.due_date,
            fact_ar_invoice_lineitem.tax_registration_number,
            fact_ar_invoice_lineitem.dest_country,
            fact_ar_invoice_lineitem.reason_code,
            fact_ar_invoice_lineitem.invoice_status,
            fact_ar_invoice_lineitem.invoice_ref_1,
            fact_ar_invoice_billing_items.sales_order as sales_order,
            fact_ar_invoice_billing_items.sales_order_item as sales_order_line,
            fact_ar_invoice_lineitem.activity_center,
            fact_ar_invoice_lineitem.site,
            fact_ar_invoice_lineitem.payment_ref,
            fact_ar_invoice_lineitem.functional_area,
            fact_ar_invoice_lineitem.net_payment_terms_period,
            fact_ar_invoice_lineitem.days_1,
            fact_ar_invoice_lineitem.days_2,
            fact_ar_invoice_lineitem.src_system_name,
            fact_ar_invoice_lineitem.dr_cr_ind,
            --Dvivde the amounts by count of lineitems for each billing doc.. Amount is picked from BSID/AD
            fact_accounting_document_header_sap.invoice_type as document_type,
            fact_accounting_document_header_sap.period as posting_period,
            fact_accounting_document_header_sap.reference_number as reference,
            fact_accounting_document_header_sap.parent_reversal_id
                as reversal_document,
            fact_accounting_document_header_sap.year as reversal_year,
            fact_accounting_document_header_sap.invoice_description
                as header_text,
            fact_accounting_document_header_sap.invoice_currency_code
                as currency,
            fact_accounting_document_header_sap.local_currency
                as local_currency,
            fact_accounting_document_header_sap.exchange_rate as exchange_rate,
            fact_accounting_document_header_sap.le_number as le_number,
            fact_accounting_document_header_sap.reference_key as reference_key,
            fact_accounting_document_header_sap.local_currency2
                as local_currency_2,
            fact_accounting_document_header_sap.reversal_flag as reversal_flag,
            fact_accounting_document_header_sap.reverse_posting_date
                as reversal_date,
            fact_accounting_document_header_sap.reversal_indicator
                as reversal_ind,
            fact_accounting_document_header_sap.ledger as ledger,
            (fact_ar_invoice_lineitem.amount_in_lc / b.count_billing_doc)
                as amount_in_lc,
            (fact_ar_invoice_lineitem.amount_in_gc / b.count_billing_doc)
                as amount_in_gc,
            (fact_ar_invoice_lineitem.lc_tax / b.count_billing_doc) as lc_tax,
            (fact_ar_invoice_lineitem.tax_original / b.count_billing_doc)
                as tax_original,
            (fact_ar_invoice_lineitem.tax_amt / b.count_billing_doc) as tax_amt,
            (fact_ar_invoice_lineitem.tax_amt_tax_curr / b.count_billing_doc)
                as tax_amt_tax_curr,
            (
                fact_ar_invoice_lineitem.tax_original_in_curr1
                / b.count_billing_doc
            ) as tax_original_in_curr1,
            (
                fact_ar_invoice_lineitem.tax_original_in_curr2
                / b.count_billing_doc
            ) as tax_original_in_curr2,
            (fact_ar_invoice_lineitem.lc2_amount / b.count_billing_doc)
                as lc2_amount
    from {{ source('SAP', 'FACT_AR_INVOICE_BILLING_ITEMS') }}
    where
        src_system_name = 'sap'
        and COALESCE(slt_delete, '') != 'X'
)

select * from source_data
