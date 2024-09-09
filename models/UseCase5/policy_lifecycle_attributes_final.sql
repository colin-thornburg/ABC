{{ config(materialized='table') }}

WITH policy_base AS (
    SELECT * FROM {{ ref('seed_policy_base') }}
),

policy_details AS (
    SELECT * FROM {{ ref('int_policy_details') }}
),

new_business_details AS (
    SELECT * FROM {{ ref('int_new_business_details') }}
),

lost_business_details AS (
    SELECT * FROM {{ ref('int_lost_business_details') }}
),

market_status AS (
    SELECT * FROM {{ ref('int_market_status') }}
),

d_policy_lifecycle_status AS (
    SELECT * FROM {{ ref('seed_d_policy_lifecycle_status') }}
),

final AS (
    SELECT
        pb.policy_key,
        COALESCE(pls_active.policy_lifecycle_status_key, -1) AS active_policy_status_key,
        COALESCE(pls_new_client.policy_lifecycle_status_key, -1) AS ajg_new_client_status_key,
        COALESCE(pls_lost_business.policy_lifecycle_status_key, -1) AS ajg_lost_business_detail_key,
        COALESCE(pls_lost_client.policy_lifecycle_status_key, -1) AS ajg_lost_client_status_key,
        COALESCE(pls_new_business.policy_lifecycle_status_key, -1) AS ajg_new_business_detail_key,
        COALESCE(pls_fee_converted_lost.policy_lifecycle_status_key, -1) AS fee_converted_lost_detail_key,
        COALESCE(pls_fee_converted_new.policy_lifecycle_status_key, -1) AS fee_converted_new_detail_key,
        COALESCE(pls_fee_split_lost.policy_lifecycle_status_key, -1) AS fee_split_lost_detail_key,
        COALESCE(pls_fee_split_new.policy_lifecycle_status_key, -1) AS fee_split_new_detail_key,
        COALESCE(pls_future_market.policy_lifecycle_status_key, -1) AS future_market_type_key,
        COALESCE(pls_market_status.policy_lifecycle_status_key, -1) AS market_status_key,
        COALESCE(pls_prior_market.policy_lifecycle_status_key, -1) AS prior_market_type_key,
        COALESCE(pls_package_converted_lost.policy_lifecycle_status_key, -1) AS package_converted_lost_detail_key,
        COALESCE(pls_package_converted_new.policy_lifecycle_status_key, -1) AS package_converted_new_detail_key,
        COALESCE(pls_policy_occurrence.policy_lifecycle_status_key, -1) AS policy_occurrence_key,
        COALESCE(ms.prior_market_key, -1) AS prior_market_key,
        COALESCE(ms.future_market_key, -1) AS future_market_key,
        pd.active_policy_status_code,
        nbd.ajg_new_client_status_code,
        lbd.ajg_lost_business_detail_code,
        lbd.ajg_lost_client_status_code,
        nbd.ajg_new_business_detail_code,
        ms.market_status_code,
        ms.prior_market_type_code,
        ms.future_market_type_code,
        CASE
            WHEN pd.policy_start_date = pd.policy_end_date OR
                 pd.policy_start_date = DATEADD(day, -1, pd.policy_end_date) OR
                 EXTRACT(YEAR FROM pd.policy_end_date) = 2099 OR
                 pb.producer_non_recurring_policy_ind
            THEN 'Non-recurring'
            ELSE 'Recurring'
        END AS policy_occurrence_code
    FROM policy_base pb
    LEFT JOIN policy_details pd ON pb.policy_key = pd.policy_key
    LEFT JOIN new_business_details nbd ON pb.policy_key = nbd.policy_key
    LEFT JOIN lost_business_details lbd ON pb.policy_key = lbd.policy_key
    LEFT JOIN market_status ms ON pb.policy_key = ms.policy_key
    LEFT JOIN d_policy_lifecycle_status pls_active 
        ON pd.active_policy_status_code = pls_active.policy_lifecycle_status_code 
        AND pls_active.policy_lifecycle_status_type_code = 'Active Policy Status'
    LEFT JOIN d_policy_lifecycle_status pls_new_client 
        ON nbd.ajg_new_client_status_code = pls_new_client.policy_lifecycle_status_code 
        AND pls_new_client.policy_lifecycle_status_type_code = 'AJG New Client Status'
    LEFT JOIN d_policy_lifecycle_status pls_lost_business 
        ON lbd.ajg_lost_business_detail_code = pls_lost_business.policy_lifecycle_status_code 
        AND pls_lost_business.policy_lifecycle_status_type_code = 'AJG Lost Business Detail'
    LEFT JOIN d_policy_lifecycle_status pls_lost_client 
        ON lbd.ajg_lost_client_status_code = pls_lost_client.policy_lifecycle_status_code 
        AND pls_lost_client.policy_lifecycle_status_type_code = 'AJG Lost Client Status'
    LEFT JOIN d_policy_lifecycle_status pls_new_business 
        ON nbd.ajg_new_business_detail_code = pls_new_business.policy_lifecycle_status_code 
        AND pls_new_business.policy_lifecycle_status_type_code = 'AJG New Business Detail'
    LEFT JOIN d_policy_lifecycle_status pls_fee_converted_lost
        ON pls_fee_converted_lost.policy_lifecycle_status_code = 'Default'
        AND pls_fee_converted_lost.policy_lifecycle_status_type_code = 'Fee Converted Lost Detail'
    LEFT JOIN d_policy_lifecycle_status pls_fee_converted_new
        ON pls_fee_converted_new.policy_lifecycle_status_code = 'Default'
        AND pls_fee_converted_new.policy_lifecycle_status_type_code = 'Fee Converted New Detail'
    LEFT JOIN d_policy_lifecycle_status pls_fee_split_lost
        ON pls_fee_split_lost.policy_lifecycle_status_code = 'Default'
        AND pls_fee_split_lost.policy_lifecycle_status_type_code = 'Fee Split Lost Detail'
    LEFT JOIN d_policy_lifecycle_status pls_fee_split_new
        ON pls_fee_split_new.policy_lifecycle_status_code = 'Default'
        AND pls_fee_split_new.policy_lifecycle_status_type_code = 'Fee Split New Detail'
    LEFT JOIN d_policy_lifecycle_status pls_future_market
        ON ms.future_market_type_code = pls_future_market.policy_lifecycle_status_code
        AND pls_future_market.policy_lifecycle_status_type_code = 'Future Market Type'
    LEFT JOIN d_policy_lifecycle_status pls_market_status
        ON ms.market_status_code = pls_market_status.policy_lifecycle_status_code
        AND pls_market_status.policy_lifecycle_status_type_code = 'Market Status'
    LEFT JOIN d_policy_lifecycle_status pls_prior_market
        ON ms.prior_market_type_code = pls_prior_market.policy_lifecycle_status_code
        AND pls_prior_market.policy_lifecycle_status_type_code = 'Prior Market Type'
    LEFT JOIN d_policy_lifecycle_status pls_package_converted_lost
        ON pls_package_converted_lost.policy_lifecycle_status_code = 'Default'
        AND pls_package_converted_lost.policy_lifecycle_status_type_code = 'Package Converted Lost Detail'
    LEFT JOIN d_policy_lifecycle_status pls_package_converted_new
        ON pls_package_converted_new.policy_lifecycle_status_code = 'Default'
        AND pls_package_converted_new.policy_lifecycle_status_type_code = 'Package Converted New Detail'
    LEFT JOIN d_policy_lifecycle_status pls_policy_occurrence
        ON pls_policy_occurrence.policy_lifecycle_status_code = 
            CASE
                WHEN pd.policy_start_date = pd.policy_end_date OR
                     pd.policy_start_date = DATEADD(day, -1, pd.policy_end_date) OR
                     EXTRACT(YEAR FROM pd.policy_end_date) = 2099 OR
                     pb.producer_non_recurring_policy_ind
                THEN 'Non-recurring'
                ELSE 'Recurring'
            END
        AND pls_policy_occurrence.policy_lifecycle_status_type_code = 'Policy Occurrence'
)

SELECT * FROM final