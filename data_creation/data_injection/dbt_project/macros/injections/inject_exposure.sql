{% macro inject_exposures(exposures) %}
    {% set relation =  source('elementary', 'elementary_exposures') %}
    {% do  elementary.edr_log(relation) %}
    {% do elementary.insert_rows(relation, exposures, True) %}
{% endmacro %}
