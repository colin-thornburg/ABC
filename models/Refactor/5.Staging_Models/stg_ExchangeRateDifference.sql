With ExchangeRateDifference AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    WHERE Account_Type = 'S'
      AND Transaction_Key = 'KDF'
      AND ifnull(slt_delete_s, '') <> 'X'
)

Select * from ExchangeRateDifference