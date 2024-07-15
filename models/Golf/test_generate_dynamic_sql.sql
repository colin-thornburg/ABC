-- models/select_users.sql

{{ config(materialized='view') }}

{{ generate_select_sql('users') }}
