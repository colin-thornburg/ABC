Select * from {{ ref('int_ar_invoice_billing_items') }}
union ALL
Select * from {{ ref('int_ar_invoice_line_items') }}