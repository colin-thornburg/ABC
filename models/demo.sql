Select {{ dbt_utils.generate_surrogate_key(['order_id', 'customer_id']) }} from {{ ref('order_analytics') }}