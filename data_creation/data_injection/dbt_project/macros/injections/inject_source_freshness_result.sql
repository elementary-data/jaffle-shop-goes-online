{% macro inject_source_freshness_result(model_id, max_loaded_at, max_loaded_at_time_ago_in_s, status, warn_after, error_after) %}
    {% set rows_to_insert = {'dbt_source_freshness_results': []} %}

    {% set test_results_relation = elementary.get_elementary_relation('dbt_source_freshness_results') %}
    {% set test_result_id = data_injection.generate_uuid() %}
    {% set result = {
        'source_freshness_execution_id': ['demo_generated_freshness', data_injection.generate_uuid()] | join('_'),
        'unique_id': model_id,
        'max_loaded_at': max_loaded_at,
        'snapshotted_at': None,
        'max_loaded_at_time_ago_in_s': max_loaded_at_time_ago_in_s,
        'status': status,
        'error': None,
        'warn_after': tojson(warn_after),
        'error_after': tojson(error_after),
        'filter': None,
        'generated_at': elementary.datetime_now_utc_as_string(),
        'invocation_id': data_injection.generate_uuid(),
        'compile_started_at': elementary.datetime_now_utc_as_string(),
        'compile_completed_at': elementary.datetime_now_utc_as_string(),
        'execute_started_at': elementary.datetime_now_utc_as_string(),
        'execute_completed_at': elementary.datetime_now_utc_as_string(),
    } %}
    {% do elementary.insert_rows(test_results_relation, [result], true) %}
    {% do rows_to_insert['dbt_source_freshness_results'].append(result) %}

    {% do return(rows_to_insert) %}
{% endmacro %}


