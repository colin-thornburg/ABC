with DiscountTakenUnearned AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    WHERE Account_Type = 'S'
      AND general_ledger_account = '0004010015'
      AND ifnull(slt_delete_s, '') <> 'X'
)

Select * from DiscountTakenUnearned