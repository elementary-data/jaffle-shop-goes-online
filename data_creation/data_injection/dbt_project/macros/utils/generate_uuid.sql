{% macro generate_uuid() %}
    {% set uuid_template = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" %}
    {% set uuid_chars = [] %}
    {% for char in uuid_template %}
        {% do uuid_chars.append(char | replace('x', [0,1,2,3,4,5,6,7,8,9,'a','b','c','d','e','f'] | random )) %}
    {% endfor %}
    {% do return(uuid_chars | join) %}
{% endmacro %}
