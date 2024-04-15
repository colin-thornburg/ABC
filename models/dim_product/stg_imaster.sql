{{ config(materialized='view') }}
{{ cast_and_coalesce_columns('imaster') }}
