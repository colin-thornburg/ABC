
show terse schemas in database DOUG_DEMO_V2
    limit 10000;
show objects in DOUG_DEMO_V2.dbt_dguthrie limit 10000;
show objects in doug_demo_v2.snapshots limit 10000;
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_part_suppliers
  
   as (
    with source as (

    select * from doug_demo_v2.tpch.partsupp

),

renamed as (

    select
    
        md5(cast(coalesce(cast(ps_partkey as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ps_suppkey as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) 
                as part_supplier_key,
        ps_partkey as part_key,
        ps_suppkey as supplier_key,
        ps_availqty as available_quantity,
        ps_supplycost as cost,
        ps_comment as comment

    from source

)

select * from renamed
  );
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

with meet_condition as(
  select *
  from doug_demo_v2.tpch.part
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not p_retailprice >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not p_retailprice <= 2000
)

select *
from validation_errors


      
    ) dbt_internal_test;
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_line_items
  
   as (
    with source as (

    select * from doug_demo_v2.tpch.lineitem

),

renamed as (

    select
    
        md5(cast(coalesce(cast(l_orderkey as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(l_linenumber as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))
                as order_item_key,
        l_orderkey as order_key,
        l_partkey as part_key,
        l_suppkey as supplier_key,
        l_linenumber as line_number,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount_percentage,
        l_tax as tax_rate,
        l_returnflag as return_flag,
        l_linestatus as status_code,
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date,
        l_shipinstruct as ship_instructions,
        l_shipmode as ship_mode,
        l_comment as comment

    from source

)

select * from renamed
  );
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select o_orderkey
from doug_demo_v2.tpch.orders
where o_orderkey is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select p_partkey
from doug_demo_v2.tpch.part
where p_partkey is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select s_suppkey
from doug_demo_v2.tpch.supplier
where s_suppkey is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select l_orderkey as from_field
    from doug_demo_v2.tpch.lineitem
    where l_orderkey is not null
),

parent as (
    select o_orderkey as to_field
    from doug_demo_v2.tpch.orders
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select l_partkey as from_field
    from doug_demo_v2.tpch.lineitem
    where l_partkey is not null
),

parent as (
    select p_partkey as to_field
    from doug_demo_v2.tpch.part
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select l_suppkey as from_field
    from doug_demo_v2.tpch.lineitem
    where l_suppkey is not null
),

parent as (
    select s_suppkey as to_field
    from doug_demo_v2.tpch.supplier
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select ps_partkey as from_field
    from doug_demo_v2.tpch.partsupp
    where ps_partkey is not null
),

parent as (
    select p_partkey as to_field
    from doug_demo_v2.tpch.part
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select o_custkey as from_field
    from doug_demo_v2.tpch.orders
    where o_custkey is not null
),

parent as (
    select c_custkey as to_field
    from doug_demo_v2.tpch.customer
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select ps_suppkey as from_field
    from doug_demo_v2.tpch.partsupp
    where ps_suppkey is not null
),

parent as (
    select s_suppkey as to_field
    from doug_demo_v2.tpch.supplier
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select s_nationkey as from_field
    from doug_demo_v2.tpch.supplier
    where s_nationkey is not null
),

parent as (
    select n_nationkey as to_field
    from doug_demo_v2.tpch.nation
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    o_orderkey as unique_field,
    count(*) as n_records

from doug_demo_v2.tpch.orders
where o_orderkey is not null
group by o_orderkey
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    s_suppkey as unique_field,
    count(*) as n_records

from doug_demo_v2.tpch.supplier
where s_suppkey is not null
group by s_suppkey
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    p_partkey as unique_field,
    count(*) as n_records

from doug_demo_v2.tpch.part
where p_partkey is not null
group by p_partkey
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select part_supplier_key
from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_part_suppliers
where part_supplier_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    part_supplier_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_part_suppliers
where part_supplier_key is not null
group by part_supplier_key
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_item_key
from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_line_items
where order_item_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    order_item_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_line_items
where order_item_key is not null
group by order_item_key
having count(*) > 1



      
    ) dbt_internal_test;
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_orders
  
   as (
    with source as (

    select * from doug_demo_v2.tpch.orders

),

renamed as (

    select

        o_orderkey as order_key,
        o_custkey as customer_key,
        o_orderstatus as status_code,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_clerk as clerk_name,
        o_orderpriority as priority_code,
        o_shippriority as ship_priority,
        o_comment as comment

    from source

)

select * from renamed
  );
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_suppliers
  
   as (
    with source as (

    select * from doug_demo_v2.tpch.supplier

),

renamed as (

    select

        s_suppkey as supplier_key,
        s_name as supplier_name,
        s_address as supplier_address,
        s_nationkey as nation_key,
        s_phone as phone_number,
        s_acctbal as account_balance,
        s_comment as comment

    from source

)

select * from renamed
  );
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_parts
  
   as (
    with source as (

    select * from doug_demo_v2.tpch.part

),

renamed as (

    select

        p_partkey as part_key,
        p_name as name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as type,
        p_size as size,
        p_container as container,
        p_retailprice as retail_price,
        p_comment as comment,
        'hello world' as col

    from source

)

select * from renamed
  );
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      




select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_suppliers where account_balance < 0


      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select supplier_key
from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_suppliers
where supplier_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    supplier_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_suppliers
where supplier_key is not null
group by supplier_key
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      




select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_orders where total_price < 0


      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

with orders as (select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_orders)

select *
from orders
where total_price < 0
      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_key
from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_orders
where order_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    order_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_orders
where order_key is not null
group by order_key
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select part_key
from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_parts
where part_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    part_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_parts
where part_key is not null
group by part_key
having count(*) > 1



      
    ) dbt_internal_test;
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.order_items
  
   as (
    with orders as (
    
    select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_orders

),

line_item as (

    select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_line_items

)
select 

    line_item.order_item_key,
    orders.order_key,
    orders.customer_key,
    line_item.part_key,
    line_item.supplier_key,
    orders.order_date,
    orders.status_code as order_status_code,


    line_item.return_flag,
    
    line_item.line_number,
    line_item.status_code as order_item_status_code,
    line_item.ship_date,
    line_item.commit_date,
    line_item.receipt_date,
    line_item.ship_mode,
    line_item.extended_price,
    line_item.quantity,
    
    -- extended_price is actually the line item total,
    -- so we back out the extended price per item
    (line_item.extended_price/nullif(line_item.quantity, 0))::decimal(16,4) as base_price,
    line_item.discount_percentage,
    (base_price * (1 - line_item.discount_percentage))::decimal(16,4) as discounted_price,

    line_item.extended_price as gross_item_sales_amount,
    (line_item.extended_price * (1 - line_item.discount_percentage))::decimal(16,4) as discounted_item_sales_amount,
    -- We model discounts as negative amounts
    (-1 * line_item.extended_price * line_item.discount_percentage)::decimal(16,4) as item_discount_amount,
    line_item.tax_rate,
    ((gross_item_sales_amount + item_discount_amount) * line_item.tax_rate)::decimal(16,4) as item_tax_amount,
    (
        gross_item_sales_amount + 
        item_discount_amount + 
        item_tax_amount
    )::decimal(16,4) as net_item_sales_amount

from
    orders
inner join line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date
  );
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.part_suppliers
  
   as (
    with part as (

    select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_parts

),

supplier as (

    select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_suppliers

),

part_supplier as (

    select * from DOUG_DEMO_V2.dbt_dguthrie.stg_tpch_part_suppliers

),

final as (
    select

        part_supplier.part_supplier_key,
        part.part_key,
        part.name as part_name,
        part.manufacturer,
        part.brand,
        part.type as part_type,
        part.size as part_size,
        part.container,
        part.retail_price,

        supplier.supplier_key,
        supplier.supplier_name,
        supplier.supplier_address,
        supplier.phone_number,
        supplier.account_balance,
        supplier.nation_key,

        part_supplier.available_quantity,
        part_supplier.cost
    from
        part
    inner join
        part_supplier
        on part.part_key = part_supplier.part_key
    inner join
        supplier
        on part_supplier.supplier_key = supplier.supplier_key
    order by
        part.part_key
)

select * from final
  );
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    order_item_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.order_items
where order_item_key is not null
group by order_item_key
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_item_key
from DOUG_DEMO_V2.dbt_dguthrie.order_items
where order_item_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select part_supplier_key
from DOUG_DEMO_V2.dbt_dguthrie.part_suppliers
where part_supplier_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    part_supplier_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.part_suppliers
where part_supplier_key is not null
group by part_supplier_key
having count(*) > 1



      
    ) dbt_internal_test;
create or replace transient table DOUG_DEMO_V2.dbt_dguthrie.fct_order_items
         as
        (

with order_item as (

    select * from DOUG_DEMO_V2.dbt_dguthrie.order_items

),

part_supplier as (

    select * from DOUG_DEMO_V2.dbt_dguthrie.part_suppliers

),

final as (
    select
        order_item.order_item_key,
        order_item.order_key,
        order_item.order_date,
        order_item.customer_key,
        order_item.part_key,
        order_item.supplier_key,
        order_item.order_item_status_code,
        order_item.return_flag,
        order_item.line_number,
        order_item.ship_date,
        order_item.commit_date,
        order_item.receipt_date,
        order_item.ship_mode,
        part_supplier.cost as supplier_cost,
        
        order_item.base_price,
        order_item.discount_percentage,
        order_item.discounted_price,
        order_item.tax_rate,

        1 as order_item_count,
        order_item.quantity,
        order_item.discounted_item_sales_amount,
        order_item.item_discount_amount,
        order_item.item_tax_amount,
        order_item.net_item_sales_amount,
        order_item.gross_item_sales_amount*3 as gross_item_sales_amount

    from
        order_item
    inner join part_supplier
        on order_item.part_key = part_supplier.part_key
            and order_item.supplier_key = part_supplier.supplier_key
)

select *
from
    final
order by
    order_date
        );
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_item_key
from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items
where order_item_key is null



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    order_item_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items
where order_item_key is not null
group by order_item_key
having count(*) > 1



      
    ) dbt_internal_test;
select ship_mode from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items group by 1;
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.agg_ship_modes_hardcoded_pivot
  
   as (
    

with merged as (
    select
        ship_mode,
        gross_item_sales_amount,
        date_part('year', order_date) as order_year
    from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items
)

select *
from
    merged
    -- have to manually map strings in the pivot operation
pivot (sum(gross_item_sales_amount) for ship_mode in (
    'AIR',
    'REG AIR',
    'FOB',
    'RAIL',
    'MAIL',
    'SHIP',
    'TRUCK'
)) as p

order by order_year
  );
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.use_variables
  
   as (
    -- This is here to show that data older than start_date exists - run this first
-- select min(order_date) from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items

-- start_date is defined in the dbt_project.yml 
-- to illustrate overriding variables from the command line, run dbt run -m use_variables --vars '{"start_date": "1996-01-01"}'
select *
from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items
where order_date >= '1999-01-01'
  );
create or replace   view DOUG_DEMO_V2.dbt_dguthrie.agg_ship_modes_dynamic_pivot
  
   as (
    



select
    date_part('year', order_date) as order_year,

    sum(case when ship_mode = 'FOB' then gross_item_sales_amount end) as "FOB_amount",
    sum(case when ship_mode = 'TRUCK' then gross_item_sales_amount end) as "TRUCK_amount",
    sum(case when ship_mode = 'REG AIR' then gross_item_sales_amount end) as "REG_AIR_amount",
    sum(case when ship_mode = 'AIR' then gross_item_sales_amount end) as "AIR_amount",
    sum(case when ship_mode = 'MAIL' then gross_item_sales_amount end) as "MAIL_amount",
    sum(case when ship_mode = 'SHIP' then gross_item_sales_amount end) as "SHIP_amount",
    sum(case when ship_mode = 'RAIL' then gross_item_sales_amount end) as "RAIL_amount"
    

from DOUG_DEMO_V2.dbt_dguthrie.fct_order_items
group by 1
  );
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    order_item_key as unique_field,
    count(*) as n_records

from DOUG_DEMO_V2.dbt_dguthrie.use_variables
where order_item_key is not null
group by order_item_key
having count(*) > 1



      
    ) dbt_internal_test;
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_item_key
from DOUG_DEMO_V2.dbt_dguthrie.use_variables
where order_item_key is null



      
    ) dbt_internal_test;