WITH FactArInvoiceLineItem AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_AR_INVOICE_LINEITEM') }}
    WHERE src_system_name = 'sap'
      AND ifnull(slt_delete, '') <> 'X'
),
FactAccountingDocumentHeaderSap AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_HEADER') }}
    WHERE invoice_type NOT IN ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')
),
FactArInvoiceBillingItems AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_AR_INVOICE_BILLING_ITEMS') }}
    WHERE src_system_name = 'sap'
      AND ifnull(slt_delete, '') <> 'X'
),
DimCustomer AS (
    SELECT *
    FROM {{ source('LIVE', 'dim_customer') }}
),
DimPaymentTerms AS (
    SELECT *
    FROM {{ source('LIVE', 'dim_payment_terms') }}
    WHERE acct_type_s = 'D'
      AND ifnull(slt_delete, '') <> 'X'
      AND active_flag_s = 'Y'
),
XxetnMapUnitSapOra AS (
    SELECT *
    FROM {{ source('fdh_slt_raw', 'dim_xxetn_map_unit_sap_ora') }}
    WHERE ifnull(sap_company_code, '') <> ''
      AND active_flag = 'Y'
),
DimBuHierarchy AS (
    SELECT *
    FROM {{ source('fdh_slt_raw', 'dim_bu_heirarchy') }}
    WHERE active_flag = 'Y'
),
MinDocumentLineNumber AS (
    SELECT FISCAL_YEAR, LEGAL_ENTITY, DOCUMENT_NUMBER, Min(DOCUMENT_LINE_NUMBER) AS min_DOCUMENT_LINE_NUMBER
    FROM FactArInvoiceLineItem
    GROUP BY FISCAL_YEAR, LEGAL_ENTITY, DOCUMENT_NUMBER
),
DiscountTakenUnearned AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    WHERE Account_Type = 'S'
      AND general_ledger_account = '0004010015'
      AND ifnull(slt_delete_s, '') <> 'X'
),
ExchangeRateDifference AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    WHERE Account_Type = 'S'
      AND Transaction_Key = 'KDF'
      AND ifnull(slt_delete_s, '') <> 'X'
),
FreightAmount AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    WHERE Account_Type = 'S'
      AND general_ledger_account = '0004020005'
      AND ifnull(slt_delete_s, '') <> 'X'
),
CashTolerance AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    WHERE Account_Type = 'S'
      AND general_ledger_account = '0004010020'
      AND ifnull(slt_delete_s, '') <> 'X'
)

-- First Intermediate Model
SELECT *
FROM FactArInvoiceLineItem
JOIN FactAccountingDocumentHeaderSap
  ON FactArInvoiceLineItem.LEGAL_ENTITY = FactAccountingDocumentHeaderSap.le_number
  AND FactArInvoiceLineItem.DOCUMENT_NUMBER = FactAccountingDocumentHeaderSap.Document_Number
  AND FactArInvoiceLineItem.Fiscal_Year = FactAccountingDocumentHeaderSap.Fiscal_Year
LEFT JOIN MinDocumentLineNumber
  ON FactArInvoiceLineItem.LEGAL_ENTITY = MinDocumentLineNumber.LEGAL_ENTITY
  AND FactArInvoiceLineItem.fiscal_year = MinDocumentLineNumber.fiscal_year
  AND FactArInvoiceLineItem.DOCUMENT_NUMBER = MinDocumentLineNumber.DOCUMENT_NUMBER
LEFT JOIN DiscountTakenUnearned
  ON FactArInvoiceLineItem.LEGAL_ENTITY = DiscountTakenUnearned.le_number
  AND FactArInvoiceLineItem.fiscal_year = DiscountTakenUnearned.fiscal_year
  AND FactArInvoiceLineItem.DOCUMENT_NUMBER = DiscountTakenUnearned.DOCUMENT_NUMBER
LEFT JOIN ExchangeRateDifference
  ON FactArInvoiceLineItem.LEGAL_ENTITY = ExchangeRateDifference.le_number
  AND FactArInvoiceLineItem.fiscal_year = ExchangeRateDifference.fiscal_year
  AND FactArInvoiceLineItem.DOCUMENT_NUMBER = ExchangeRateDifference.DOCUMENT_NUMBER
LEFT JOIN FreightAmount
  ON FactArInvoiceLineItem.LEGAL_ENTITY = FreightAmount.le_number
  AND FactArInvoiceLineItem.fiscal_year = FreightAmount.fiscal_year
  AND FactArInvoiceLineItem.DOCUMENT_NUMBER = FreightAmount.DOCUMENT_NUMBER
LEFT JOIN CashTolerance
  ON FactArInvoiceLineItem.LEGAL_ENTITY = CashTolerance.le_number
  AND FactArInvoiceLineItem.fiscal_year = CashTolerance.fiscal_year
  AND FactArInvoiceLineItem.DOCUMENT_NUMBER = CashTolerance.DOCUMENT_NUMBER
LEFT JOIN DimCustomer
  ON FactArInvoiceLineItem.Customer = DimCustomer.party_number_s
  AND FactArInvoiceLineItem.LEGAL_ENTITY = DimCustomer.le_number_s
  AND FactArInvoiceLineItem.src_system_name = DimCustomer.src_system_name_s
LEFT JOIN XxetnMapUnitSapOra
  ON FactArInvoiceLineItem.legal_entity = XxetnMapUnitSapOra.sap_company_code
  AND FactArInvoiceLineItem.SITE = XxetnMapUnitSapOra.sap_profit_center
LEFT JOIN DimBuHierarchy
  ON XxetnMapUnitSapOra.site = DimBuHierarchy.ledger
LEFT JOIN DimPaymentTerms
  ON FactArInvoiceLineItem.PAY_TERM = DimPaymentTerms.TERM_ID_s
  AND CASE
      WHEN DimPaymentTerms.count_of_date_type > 1 THEN day(FactArInvoiceLineItem.BASELINE_DATE)
      WHEN (DimPaymentTerms.date_type = 'D' AND DimPaymentTerms.MinDayLimitByPartition = 0 AND DimPaymentTerms.MaxDayLimitByPartition = 0) THEN 0
      WHEN (DimPaymentTerms.date_type = 'D') THEN day(FactArInvoiceLineItem.posting_date)
      WHEN (DimPaymentTerms.date_type = 'B' AND DimPaymentTerms.MinDayLimitByPartition = 0 AND DimPaymentTerms.MaxDayLimitByPartition = 0) THEN 0
      WHEN (DimPaymentTerms.date_type = 'B') THEN day(FactArInvoiceLineItem.DOCUMENT_DATE)
      WHEN (DimPaymentTerms.date_type = 'C' AND DimPaymentTerms.MinDayLimitByPartition = 0 AND DimPaymentTerms.MaxDayLimitByPartition = 0) THEN 0
      WHEN (DimPaymentTerms.date_type = 'C') THEN day(FactArInvoiceLineItem.entered_on)
      ELSE 0
      END BETWEEN DimPaymentTerms.min_day_limit_s AND DimPaymentTerms.max_day_limit_s
WHERE FactArInvoiceLineItem.fiscal_year >= 2022
GROUP BY FactArInvoiceLineItem.LEGAL_ENTITY, FactArInvoiceLineItem.DOCUMENT_NUMBER
ORDER BY FactArInvoiceLineItem.POSTING_DATE DESC

UNION ALL

-- Second Intermediate Model
SELECT *
FROM FactArInvoiceBillingItems
JOIN FactAccountingDocumentHeaderSap
  ON FactArInvoiceBillingItems.LEGAL_ENTITY = FactAccountingDocumentHeaderSap.le_number
  AND FactArInvoiceBillingItems.DOCUMENT_NUMBER = FactAccountingDocumentHeaderSap.Document_Number
  AND FactArInvoiceBillingItems.Fiscal_Year = FactAccountingDocumentHeaderSap.Fiscal_Year
LEFT JOIN DimCustomer
  ON FactArInvoiceBillingItems.Customer = DimCustomer.party_number_s
  AND FactArInvoiceBillingItems.LEGAL_ENTITY = DimCustomer.le_number_s
  AND FactArInvoiceBillingItems.src_system_name = DimCustomer.src_system_name_s
LEFT JOIN XxetnMapUnitSapOra
  ON FactArInvoiceBillingItems.legal_entity = XxetnMapUnitSapOra.sap_company_code
  AND FactArInvoiceBillingItems.SITE = XxetnMapUnitSapOra.sap_profit_center
LEFT JOIN DimBuHierarchy
  ON XxetnMapUnitSapOra.site = DimBuHierarchy.ledger
LEFT JOIN DimPaymentTerms
  ON FactArInvoiceBillingItems.PAY_TERM = DimPaymentTerms.TERM_ID_s
  AND CASE
      WHEN DimPaymentTerms.count_of_date_type > 1 THEN day(FactArInvoiceBillingItems.BASELINE_DATE)
      WHEN (DimPaymentTerms.date_type = 'D') THEN day(FactArInvoiceBillingItems.posting_date)
      WHEN (DimPaymentTerms.date_type = 'B') THEN day(FactArInvoiceBillingItems.DOCUMENT_DATE)
      WHEN (DimPaymentTerms.date_type = 'C') THEN day(FactArInvoiceBillingItems.entered_on)
      ELSE 0
      END BETWEEN DimPaymentTerms.min_day_limit_s AND DimPaymentTerms.max_day_limit_s
WHERE FactArInvoiceBillingItems.fiscal_year >= 2022
GROUP BY FactArInvoiceBillingItems.LEGAL_ENTITY, FactArInvoiceBillingItems.DOCUMENT_NUMBER
ORDER BY FactArInvoiceBillingItems.POSTING_DATE DESC;