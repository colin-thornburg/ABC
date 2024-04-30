{{
    config(
        materialized='view'
    )
}}


Select * from {{ ref('order_analytics') }}