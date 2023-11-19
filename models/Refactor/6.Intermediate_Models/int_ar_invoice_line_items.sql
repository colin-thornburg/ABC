-- This model focuses on AR invoice line items.
-- It contains transformations and joins related to line-item-level data for AR invoices,
-- including item details, quantities, prices, and other line-item-specific information.


SELECT *
FROM {{ ref('stg_FactArInvoiceLineItem') }}
INNER JOIN {{ ref('stg_FactAccountingDocumentHeaderSap') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_FactAccountingDocumentHeaderSap') }}.LE_NUMBER
        AND {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
        = {{ ref('stg_FactAccountingDocumentHeaderSap') }}.DOCUMENT_NUMBER
        AND {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR
        = {{ ref('stg_FactAccountingDocumentHeaderSap') }}.FISCAL_YEAR
LEFT JOIN {{ ref('stg_MinDocumentLineNumber') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_MinDocumentLineNumber') }}.LEGAL_ENTITY
        AND {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR
        = {{ ref('stg_MinDocumentLineNumber') }}.FISCAL_YEAR
        AND {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
        = {{ ref('stg_MinDocumentLineNumber') }}.DOCUMENT_NUMBER
LEFT JOIN {{ ref('stg_DiscountTakenUnearned') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_DiscountTakenUnearned') }}.LE_NUMBER
        AND {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR
        = {{ ref('stg_DiscountTakenUnearned') }}.FISCAL_YEAR
        AND {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
        = {{ ref('stg_DiscountTakenUnearned') }}.DOCUMENT_NUMBER
LEFT JOIN {{ ref('stg_ExchangeRateDifference') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_ExchangeRateDifference') }}.LE_NUMBER
        AND {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR
        = {{ ref('stg_ExchangeRateDifference') }}.FISCAL_YEAR
        AND {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
        = {{ ref('stg_ExchangeRateDifference') }}.DOCUMENT_NUMBER
LEFT JOIN {{ ref('stg_FreightAmount') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_FreightAmount') }}.LE_NUMBER
        AND {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR
        = {{ ref('stg_FreightAmount') }}.FISCAL_YEAR
        AND {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
        = {{ ref('stg_FreightAmount') }}.DOCUMENT_NUMBER
LEFT JOIN {{ ref('stg_CashTolerance') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_CashTolerance') }}.LE_NUMBER
        AND {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR
        = {{ ref('stg_CashTolerance') }}.FISCAL_YEAR
        AND {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
        = {{ ref('stg_CashTolerance') }}.DOCUMENT_NUMBER
LEFT JOIN {{ ref('stg_DimCustomer') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.CUSTOMER
        = {{ ref('stg_DimCustomer') }}.PARTY_NUMBER_S
        AND {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_DimCustomer') }}.LE_NUMBER_S
        AND {{ ref('stg_FactArInvoiceLineItem') }}.SRC_SYSTEM_NAME
        = {{ ref('stg_DimCustomer') }}.SRC_SYSTEM_NAME_S
LEFT JOIN {{ ref('stg_XxetnMapUnitSapOra') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY
        = {{ ref('stg_XxetnMapUnitSapOra') }}.SAP_COMPANY_CODE
        AND {{ ref('stg_FactArInvoiceLineItem') }}.SITE
        = {{ ref('stg_XxetnMapUnitSapOra') }}.SAP_PROFIT_CENTER
LEFT JOIN {{ ref('stg_DimBuHierarchy') }}
    ON
        {{ ref('stg_XxetnMapUnitSapOra') }}.SITE
        = {{ ref('stg_DimBuHierarchy') }}.LEDGER
LEFT JOIN {{ ref('stg_DimPaymentTerms') }}
    ON
        {{ ref('stg_FactArInvoiceLineItem') }}.PAY_TERM
        = {{ ref('stg_DimPaymentTerms') }}.TERM_ID_S
        AND CASE
            WHEN
                {{ ref('stg_DimPaymentTerms') }}.COUNT_OF_DATE_TYPE > 1
                THEN day({{ ref('stg_FactArInvoiceLineItem') }}.BASELINE_DATE)
            WHEN
                (
                    {{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'D'
                    AND {{ ref('stg_DimPaymentTerms') }}.MINDAYLIMITBYPARTITION
                    = 0
                    AND {{ ref('stg_DimPaymentTerms') }}.MAXDAYLIMITBYPARTITION
                    = 0
                )
                THEN 0
            WHEN
                ({{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'D')
                THEN day({{ ref('stg_FactArInvoiceLineItem') }}.POSTING_DATE)
            WHEN
                (
                    {{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'B'
                    AND {{ ref('stg_DimPaymentTerms') }}.MINDAYLIMITBYPARTITION
                    = 0
                    AND {{ ref('stg_DimPaymentTerms') }}.MAXDAYLIMITBYPARTITION
                    = 0
                )
                THEN 0
            WHEN
                ({{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'B')
                THEN day({{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_DATE)
            WHEN
                (
                    {{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'C'
                    AND {{ ref('stg_DimPaymentTerms') }}.MINDAYLIMITBYPARTITION
                    = 0
                    AND {{ ref('stg_DimPaymentTerms') }}.MAXDAYLIMITBYPARTITION
                    = 0
                )
                THEN 0
            WHEN
                ({{ ref('stg_DimPaymentTerms') }}.DATE_TYPE = 'C')
                THEN day({{ ref('stg_FactArInvoiceLineItem') }}.ENTERED_ON)
            ELSE 0
        END BETWEEN {{ ref('stg_DimPaymentTerms') }}.MIN_DAY_LIMIT_S AND {{ ref('stg_DimPaymentTerms') }}.MAX_DAY_LIMIT_S
WHERE {{ ref('stg_FactArInvoiceLineItem') }}.FISCAL_YEAR >= 2022
GROUP BY
    {{ ref('stg_FactArInvoiceLineItem') }}.LEGAL_ENTITY,
    {{ ref('stg_FactArInvoiceLineItem') }}.DOCUMENT_NUMBER
ORDER BY {{ ref('stg_FactArInvoiceLineItem') }}.POSTING_DATE DESC
