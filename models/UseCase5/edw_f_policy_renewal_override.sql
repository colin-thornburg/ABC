
    WITH
    policy_new_lost_status_override AS
    (
        SELECT
            policy_id,
            CASE
                WHEN policy_source_system_code = 'FDW'
                    THEN
                        CASE
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '1' THEN 'EPIC_US'
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '2' THEN 'EPIC_CAN'
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '3' THEN 'AIM_CF'
                            ELSE policy_source_system_code
                        END
                WHEN policy_source_system_code = 'AIM' THEN 'RPS'
                WHEN policy_source_system_code = 'IB_EDW' THEN 'UK'
                ELSE policy_source_system_code
            END AS policy_source_system_code,
            CASE
                WHEN policy_source_system_code = 'FDW'
                    THEN
                        CASE
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '1' THEN 'EPIC_US'
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '2' THEN 'EPIC_CAN'
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '3' THEN 'AIM_CF'
                            ELSE policy_source_system_instance_code
                        END
                ELSE policy_source_system_instance_code
            END AS policy_source_system_instance_code,
            renewal_policy_id,
            CASE
                WHEN renewal_policy_source_system_code = 'FDW'
                    THEN
                        CASE
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '1' THEN 'EPIC_US'
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '2' THEN 'EPIC_CAN'
                            WHEN TRIM(SPLIT_PART(policy_source_system_instance_code, '-', 2)) = '3' THEN 'AIM_CF'
                            ELSE renewal_policy_source_system_code
                        END
                WHEN renewal_policy_source_system_code = 'AIM' THEN 'RPS'
                WHEN renewal_policy_source_system_code = 'IB_EDW' THEN 'UK'
                ELSE renewal_policy_source_system_code
            END AS renewal_policy_source_system_code,
            CASE
                WHEN renewal_policy_source_system_code = 'FDW'
                    THEN
                        CASE
                            WHEN TRIM(SPLIT_PART(renewal_policy_source_system_instance_code, '-', 2)) = '1' THEN 'EPIC_US'
                            WHEN TRIM(SPLIT_PART(renewal_policy_source_system_instance_code, '-', 2)) = '2' THEN 'EPIC_CAN'
                            WHEN TRIM(SPLIT_PART(renewal_policy_source_system_instance_code, '-', 2)) = '3' THEN 'AIM_CF'
                            ELSE renewal_policy_source_system_instance_code
                        END
                ELSE renewal_policy_source_system_instance_code
            END AS renewal_policy_source_system_instance_code
        FROM {{ ref('mdm_s_policy_status_new_lost_status_override') }}
        WHERE renewal_policy_id IS NOT NULL
    )
    SELECT
        d1.policy_key,
        d2.policy_key AS renewal_policy_key
    FROM
        policy_new_lost_status_override AS a
        INNER JOIN
        {{ ref('edw_d_source_system') }} AS b1
        ON a.policy_source_system_code = b1.source_system_code
        INNER JOIN
        {{ ref('edw_d_source_system_instance') }} AS c1
        ON a.policy_source_system_instance_code = c1.source_system_instance_code
        INNER JOIN
        {{ ref('edw_d_policy') }} AS d1
        ON d1.policy_id = a.policy_id
        AND d1.env_source_code = b1.source_system_code
        AND d1.data_source_code = CASE
                                      WHEN b1.source_system_code = 'FDW'
                                          THEN 'dim_policy - ' || c1.source_system_instance_id
                                      WHEN b1.source_system_code = 'RPS'
                                          THEN 'dim_quote - ' || c1.source_system_instance_id
                                      WHEN b1.source_system_code = 'UK'
                                          THEN 'policy - ' || c1.source_system_instance_id
                                      ELSE d1.data_source_code
                                  END
        INNER JOIN
        {{ ref('edw_d_source_system') }} AS b2
        ON a.renewal_policy_source_system_code = b2.source_system_code
        INNER JOIN
        {{ ref('edw_d_source_system_instance') }} AS c2
        ON a.renewal_policy_source_system_instance_code = c2.source_system_instance_code
        INNER JOIN
        {{ ref('edw_d_policy') }} AS d2
        ON d2.policy_id = a.renewal_policy_id
        AND d2.env_source_code = b2.source_system_code
        AND d2.data_source_code = CASE
                                      WHEN b1.source_system_code = 'FDW'
                                          THEN 'dim_policy - ' || c2.source_system_instance_id
                                      WHEN b1.source_system_code = 'RPS'
                                          THEN 'dim_quote - ' || c2.source_system_instance_id
                                      WHEN b1.source_system_code = 'UK'
                                          THEN 'policy - ' || c2.source_system_instance_id
                                      ELSE d2.data_source_code
                                  END