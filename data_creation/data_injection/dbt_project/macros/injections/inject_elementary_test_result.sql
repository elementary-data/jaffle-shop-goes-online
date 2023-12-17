{% macro inject_elementary_test_result(test_id, test_name, test_column_name, test_type, test_sub_type, test_params, test_timestamp, test_status, model_id, model_name, result_description, test_result_rows = [], owners = [], tags = []) %}
    {% set test_results_relation = elementary.get_elementary_relation('elementary_test_results') %}
    {% set test_result_id = data_injection.generate_uuid() %}
    {% set result = {
        'id': test_result_id,
        'data_issue_id': none,
        'test_execution_id': data_injection.generate_uuid(),
        'test_unique_id': test_id,
        'model_unique_id': model_id,
        'invocation_id': data_injection.generate_uuid(),
        'detected_at': test_timestamp,
        'created_at': test_timestamp,
        'database_name': target.database,
        'schema_name': target.schema.replace("_elementary", ""),
        'table_name': model_name,
        'column_name': test_column_name,
        'test_type': test_type,
        'test_sub_type': test_sub_type,
        'test_results_description': result_description,
        'owners': owners,
        'tags': tags,
        'test_results_query': none,
        'other': none,
        'test_name': test_name,
        'test_params': test_params | tojson,
        'severity': 'ERROR',
        'status': test_status,
        'failures': none,
        'test_short_name': test_name,
        'test_alias': test_name,
        'result_rows': none,
        'failed_row_count': none
    } %}
    {% do elementary.insert_rows(test_results_relation, [result], true) %}

    {% if test_result_rows %}
        {% set result_rows_relation = elementary.get_elementary_relation('test_result_rows') %}
        {% set db_result_rows = [] %}
        {% for test_result_row in test_result_rows %}
            {% do db_result_rows.append({
                'elementary_test_results_id': test_result_id,
                'result_row': test_result_row | tojson,
                'detected_at': test_timestamp,
                'created_at': test_timestamp
            }) %}
        {% endfor %}
        {% do elementary.insert_rows(result_rows_relation, db_result_rows, true) %}
    {% endif %}
{% endmacro %}
