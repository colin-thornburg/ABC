-- intermediate_union_model

{{ config(materialized='view') }}

with unioned_data as (
    -- First part of the union from intermediate_billing_items_aggregation
    select
        col1, col2, col3, -- Replace with actual column names from intermediate_billing_items_aggregation
        'billing_items_aggregation' as source_model
    from {{ ref('intermediate_billing_items_aggregation') }}

    union all

    -- Second part of the union from intermediate_document_lineitem_aggregation
    select
        col1, col2, col3, -- Replace with actual column names from intermediate_document_lineitem_aggregation
        'document_lineitem_aggregation' as source_model
    from {{ ref('intermediate_document_lineitem_aggregation') }}

    union all

    -- Third part of the union from intermediate_accounting_document_aggregation
    select
        col1, col2, col3, -- Replace with actual column names from intermediate_accounting_document_aggregation
        'accounting_document_aggregation' as source_model
    from {{ ref('intermediate_accounting_document_aggregation') }}

    union all

    -- Fourth part of the union from intermediate_customer_mapping
    select
        col1, col2, col3, -- Replace with actual column names from intermediate_customer_mapping
        'customer_mapping' as source_model
    from {{ ref('intermediate_customer_mapping') }}
)

select * from unioned_data
