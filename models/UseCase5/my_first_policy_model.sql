{{
    config(
        materialized='view'
    )
}}

Select * from {{ ref('revenue_detail') }}