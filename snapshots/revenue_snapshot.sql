{% snapshot revenue_snapshot %}

{{
    config(
      target_database=target.database,
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='run_timestamp',
    )
}}

select * from {{ ref('revenue') }}

{% endsnapshot %}