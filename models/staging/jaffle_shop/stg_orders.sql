{{ config(
    tags=["finance"],
    post_hook = [
            "{{snowflake_query_logging(this, audit_table_schema='audit_tables', audit_table_name = 'dbt_log_table')}}"
        ]
) }}

select
    id as order_id,
    user_id as customer_id,
    order_date,
    state
from {{ source('jaffle_shop', 'orders') }}