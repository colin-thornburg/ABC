-- models/intermediate/int_policy_EPIC_US_combined.sql

{{ config(materialized='ephemeral') }}

SELECT
    a.*,
    b.renewal_policy_key,
    c.prior_market_key,
    c.future_market_key,
    c.active_policy_status_key,
    c.ajg_new_client_status_key,
    c.ajg_lost_business_detail_key,
    c.ajg_lost_client_status_key,
    c.ajg_new_business_detail_key,
    c.fee_converted_lost_detail_key,
    c.fee_converted_new_detail_key,
    c.fee_split_lost_detail_key,
    c.fee_split_new_detail_key,
    c.future_market_type_key,
    c.market_status_key,
    c.prior_market_type_key,
    c.package_converted_lost_detail_key,
    c.package_converted_new_detail_key,
    c.policy_occurrence_key,
    c.active_policy_status_code,
    c.ajg_new_client_status_code,
    c.ajg_lost_business_detail_code,
    c.ajg_lost_client_status_code,
    c.ajg_new_business_detail_code,
    c.fee_converted_lost_detail_code,
    c.fee_converted_new_detail_code,
    c.fee_split_lost_detail_code,
    c.fee_split_new_detail_code,
    c.future_market_type_code,
    c.market_status_code,
    c.prior_market_type_code,
    c.package_converted_lost_detail_code,
    c.package_converted_new_detail_code,
    c.policy_occurrence_code
FROM {{ ref('stg_policy_EPIC_US_main') }} a
LEFT JOIN {{ ref('stg_policy_EPIC_US_renewal') }} b ON a.policy_key = b.policy_key
LEFT JOIN {{ ref('stg_policy_EPIC_US_lifecycle_attr') }} c ON a.policy_key = c.policy_key