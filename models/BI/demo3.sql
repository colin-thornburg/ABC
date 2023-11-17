select  
 {{ get_last_value('amount', ref('order_analytics')) }} as last_amount  

from {{ ref('order_analytics') }}
limit 1