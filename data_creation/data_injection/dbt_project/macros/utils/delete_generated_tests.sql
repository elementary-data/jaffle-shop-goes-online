{% macro delete_generated_tests(test_id) %}
    {% set generated_tests_query %}
    select distinct unique_id
    from {{ ref('elementary', 'dbt_tests') }}
    where meta ilike '%generated_result%'
    {% endset %}

    {% set result = run_query(generated_tests_query) %}
    {% for row in result %}
        {% set unique_id = row['unique_id'] %}
        {% do data_injection.delete_test_data(unique_id) %}
    {% endfor %}

    {% set delete_generated_source_freshness_results %}
        delete from {{ ref('elementary', 'dbt_source_freshness_results') }}
        where source_freshness_execution_id ilike 'demo_generated_freshness%'
    {% endset %}
    {% do run_query(delete_generated_source_freshness_results) %}
{% endmacro %}
