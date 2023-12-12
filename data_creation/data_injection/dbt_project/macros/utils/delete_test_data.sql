{% macro delete_test_data(test_id) %}
    {% set result_ids_query %}
    select distinct id
    from {{ ref('elementary', 'elementary_test_results') }}
    where test_unique_id = '{{ test_id }}'
    {% endset %}

    {% set result = run_query(result_ids_query) %}
    {% for row in result %}
        {% set result_id = row['id'] %}

        {% set delete_test_result_rows_query %}
        delete from {{ ref('elementary', 'test_result_rows') }}
        where elementary_test_results_id = '{{ result_id }}'
        {% endset %}
        {% do run_query(delete_test_result_rows_query) %}
    {% endfor %}

    {% set delete_test_results_query %}
    delete from {{ ref('elementary', 'elementary_test_results') }}
    where test_unique_id = '{{ test_id }}'
    {% endset %}
    {% do run_query(delete_test_results_query) %}

    {% set delete_test_query %}
        delete from {{ ref('elementary', 'dbt_tests') }}
        where unique_id = '{{ test_id }}'
    {% endset %}
    {% do run_query(delete_test_query) %}
{% endmacro %}
