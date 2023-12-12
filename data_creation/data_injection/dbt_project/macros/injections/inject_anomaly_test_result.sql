{% macro inject_anomaly_test_result(test_id, test_name, test_column_name, test_sub_type, test_params, test_timestamp,
                         test_status, model_id, model_name, test_metrics, result_description) %}
    {% set rows_to_insert = {'elementary_test_results': [], 'test_result_rows': []} %}

    {% set test_results_relation = elementary.get_elementary_relation('elementary_test_results') %}
    {% set test_result_id = data_injection.generate_uuid() %}
    {% set result = {
        'id': test_result_id,
        'data_issue_id': None,
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
        'test_type': 'anomaly_detection',
        'test_sub_type': test_sub_type,
        'test_results_description': result_description,
        'owners': '[]',
        'tags': '[]',
        'test_results_query': None,
        'other': None,
        'test_name': test_name,
        'test_params': test_params | tojson,
        'severity': 'ERROR',
        'status': test_status,
        'failures': None,
        'test_short_name': test_name,
        'test_alias': test_name,
        'result_rows': None,
        'failed_row_count': None
    } %}
    {% do rows_to_insert['elementary_test_results'].append(result) %}

    {% set result_rows_relation = elementary.get_elementary_relation('test_result_rows') %}
    {% set db_result_rows = [] %}
    {% for test_metric in test_metrics %}
        {% do db_result_rows.append({
            'elementary_test_results_id': test_result_id,
            'result_row': test_metric | tojson,
            'detected_at': test_timestamp,
            'created_at': test_timestamp
        }) %}
    {% endfor %}
    {% do rows_to_insert['test_result_rows'].extend(db_result_rows) %}

    {% do return(rows_to_insert) %}
{% endmacro %}
