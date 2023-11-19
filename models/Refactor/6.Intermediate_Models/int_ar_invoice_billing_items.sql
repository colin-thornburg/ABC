-- This model is centered around AR invoice billing items.
-- It includes transformations and joins related to billing-item-level data for AR invoices,
-- such as total amounts, taxes, discounts, and other billing-related information.


SELECT *
FROM {{ ref('stg_FactArInvoiceBillingItems') }}
INNER JOIN {{ ref('stg_FactAccountingDocumentHeaderSap') }}
    ON
        {{ ref('stg_FactArInvoiceBillingItems') }}.LEGAL_ENTITY
        = {{ ref('stg_FactAccountingDocumentHeaderSap') }}.LE_NUMBER
        AND {{ ref('stg_FactArInvoiceBillingItems') }}.DOCUMENT_NUMBER
        = {{ ref('stg_FactAccountingDocumentHeaderSap') }}.DOCUMENT_NUMBER
        AND {{ ref('stg_FactArInvoiceBillingItems') }}.FISCAL_YEAR
        = {{ ref('stg_FactAccountingDocumentHeaderSap') }}.FISCAL_YEAR
LEFT JOIN {{ ref('stg_DimCustomer') }}
    ON
        {{ ref('stg_FactArInvoiceBillingItems') }}.CUSTOMER
        = {{ ref('stg_DimCustomer') }}.PARTY_NUMBER_S
        AND {{ ref('stg_FactArInvoiceBillingItems') }}.LEGAL_ENTITY
        = {{ ref('stg_DimCustomer') }}.LE_NUMBER_S
        AND {{ ref('stg_FactArInvoiceBillingItems') }}.SRC_SYSTEM_NAME
        = {{ ref('stg_DimCustomer') }}.SRC_SYSTEM_NAME_S
LEFT JOIN {{ ref('stg_XxetnMapUnitSapOra') }}
    ON
        {{ ref('stg_FactArInvoiceBillingItems') }}.LEGAL_ENTITY
        = {{ ref('stg_XxetnMapUnitSapOra') }}.SAP_COMPANY_CODE
        AND {{ ref('stg_FactArInvoiceBillingItems') }}.SITE
        = {{ ref('stg_XxetnMapUnitSapOra') }}.SAP_PROFIT_CENTER
LEFT JOIN {{ ref('stg_DimBuHierarchy') }}
    ON
        {{ ref('stg_XxetnMapUnitSapOra') }}.SITE
        = {{ ref('stg_DimBuHierarchy') }}.LEDGER
LEFT JOIN {{ ref('stg_DimPaymentTerms') }}
    ON
        {{ ref('stg_FactArInvoiceBillingItems') }}.PAY_TERM
        = {{ ref('stg_DimPaymentTerms') }}.TERM_ID_S
        AND CASE
            WHEN
                {{ ref('stg_DimPaymentTerms') }}.COUNT_OF_DATE_TYPE > 1
                THEN
                    day(
                        {{ ref('stg_FactArInvoiceBillingItems') }}.BASELINE_DATE
                    )
            WHEN
                ({{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'D')
                THEN
                    day({{ ref('stg_FactArInvoiceBillingItems') }}.POSTING_DATE)
            WHEN
                ({{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'B')
                THEN
                    day(
                        {{ ref('stg_FactArInvoiceBillingItems') }}.DOCUMENT_DATE
                    )
            WHEN
                ({{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'C')
                THEN day({{ ref('stg_FactArInvoiceBillingItems') }}.ENTERED_ON)
            ELSE 0
        END BETWEEN {{ ref('stg_DimPaymentTerms') }}.MIN_DAY_LIMIT_S AND {{ ref('stg_DimPaymentTerms') }}.MAX_DAY_LIMIT_S
WHERE {{ ref('stg_FactArInvoiceBillingItems') }}.FISCAL_YEAR >= 2022
GROUP BY
    {{ ref('stg_FactArInvoiceBillingItems') }}.LEGAL_ENTITY,
    {{ ref('stg_FactArInvoiceBillingItems') }}.DOCUMENT_NUMBER
ORDER BY {{ ref('stg_FactArInvoiceBillingItems') }}.POSTING_DATE DESC
