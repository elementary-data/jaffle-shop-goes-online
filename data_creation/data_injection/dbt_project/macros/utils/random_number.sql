{% macro random_number(min_value = 1, max_value = 200) %}
    {% do return(range(min_value, max_value) | random) %}
{% endmacro %}