{% macro get_models_unique_ids(exclude_elementary = true, filter = none) %}
    {% set unique_ids = [] %}
    {% set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') | selectattr('package_name', '!=', 'elementary')  %}
    {% for model in models %}
        {% if filter is none or filter in model.get("fqn", []) %}
            {% do unique_ids.append(model.get("unique_id")) %}  
        {% endif %}
    {% endfor %}
    {% do return(unique_ids) %}
{% endmacro %}