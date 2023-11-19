With DimCustomer AS (
    SELECT *
    FROM {{ source('LIVE', 'dim_customer') }}
)

Select * from DimCustomer