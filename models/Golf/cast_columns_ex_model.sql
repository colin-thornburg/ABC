{{
    config(
        materialized='view'
    )
}}

{{ cast_product_columns('your_table_name_here') }}