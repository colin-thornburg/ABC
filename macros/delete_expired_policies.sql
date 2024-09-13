-- macros/delete_expired_policies.sql

{% macro delete_expired_policies() %}
    {% if execute %}
        {% set target_relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {% if target_relation is none %}
            -- Target table does not exist; skip deletion
        {% else %}
            DELETE FROM {{ this }} AS t
            USING (
                SELECT t.policy_key
                FROM {{ this }} AS t
                LEFT JOIN {{ ref('temp_edw_f_policy_EPIC_US_FINAL') }} AS s
                    ON t.policy_key = s.policy_key
                    AND s.insert_to_fact
                WHERE s.policy_key IS NULL
            ) AS y
            WHERE t.policy_key = y.policy_key;
        {% endif %}
    {% endif %}
{% endmacro %}
