{% macro mixed_int_cast(column_name, int_cast) %}
    CASE
        WHEN {{ column_name }}::text ~ '^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$'
            THEN {{ column_name }}::{{ int_cast }}
        ELSE NULL
    END
{% endmacro %}