With DimPaymentTerms AS (
    SELECT *
    FROM {{ source('LIVE', 'dim_payment_terms') }}
    WHERE acct_type_s = 'D'
      AND ifnull(slt_delete, '') <> 'X'
      AND active_flag_s = 'Y'
)

Select * from DimPaymentTerms