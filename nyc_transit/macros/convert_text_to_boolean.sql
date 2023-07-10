{% macro convert_text_to_booloean(column_name) %}
    {% set flag_map = {"Y": "TRUE", "N": "FALSE"} %}
    CASE {{ column_name }} 
    {% for key, value in flag_map.items() %}
        WHEN '{{ key }}' THEN '{{ value }}'
    {% endfor %}
    ELSE NULL END
{% endmacro %}