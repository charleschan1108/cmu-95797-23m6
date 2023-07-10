{% macro convert_na_to_null(column_name) %}
    CASE {{ column_name }} 
        WHEN 'NA' THEN NULL
    ELSE {{ column_name }} END
{% endmacro %}