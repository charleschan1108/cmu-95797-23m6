{% macro gender_code_to_text(column_name) %}
    {% set gender_map = {"0": "Unknown", "1": "Male", "2": "Female"} %}
    CASE {{ column_name }} 
    {% for key, value in gender_map.items() %}
        WHEN '{{ key }}' THEN '{{ value }}'
    {% endfor %}
    END
{% endmacro %}