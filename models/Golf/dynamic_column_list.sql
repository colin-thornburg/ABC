-- models/dynamic_column_list.sql

{{ config(materialized='view') }}

{{ generate_listagg_sql('users') }}