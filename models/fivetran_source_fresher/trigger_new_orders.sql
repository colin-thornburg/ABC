{{
    config(
        materialized='table'
    )
}}

Select 1 as id, current_timestamp as order_date
union all
Select 2 as id, current_timestamp as order_date