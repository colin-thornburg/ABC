Select 
    id, 
    null as column1, 
    column2, 
    updated_at from 
    {{ ref('my_source_2') }}



-- Make this dynamic to handle more columns like in the real world...
/*
% set columns = ['id', 'column1', 'column2', 'updated_at'] %

SELECT
    {% for column in columns %}
        {% if column == 'column1' %}
            NULL AS {{ column }},
        {% else %}
            {{ column }},
        {% endif %}
    {% endfor %}
FROM {{ ref('my_source_2') }}
*/