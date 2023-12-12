{% macro inject_dbt_test(test_id, test_name, test_column_name, test_params, description) %}
    {% set rows_to_insert = {'dbt_tests': []} %}

    {% set relation = elementary.get_elementary_relation('dbt_tests') %}
    {% set test_data = {
        'unique_id': test_id,
        'database_name': target.database,
        'schema_name': target.schema.replace("_elementary", ""),
        'name': test_name,
        'short_name': test_name,
        'alias': test_name,
        'test_column_name': test_column_name,
        'severity': 'warn',
        'warn_if': '',
        'error_if': '',
        'test_params': test_params | tojson,
        'test_namespace': 'elementary',
        'tags': '[]',
        'model_tags': '[]',
        'model_owners': '[]',
        'meta': tojson({"description": description, "generated_result": true}),
        'depends_on_macros': '[]',
        'depends_on_nodes': '[]',
        'generated_at': elementary.datetime_now_utc_as_string(),
    } %}
    {% do rows_to_insert['dbt_tests'].append(test_data) %}

    {% do return(rows_to_insert) %}
{% endmacro %}
