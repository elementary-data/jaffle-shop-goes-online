{% macro inject_test_run(test_id, generated_at, run_status, materialization, run_duration = none) %}
    {% set relation = elementary.get_elementary_relation('dbt_run_results') %}
    {% set model_run_duration = run_duration or data_injection.random_number() %}
    {% set model_run = {
        "model_execution_id": data_injection.generate_uuid(),
        "unique_id": model_id,
        "invocation_id": data_injection.generate_uuid(),
        "generated_at": generated_at,
        "created_at": generated_at,
        "name": model_id,
        "message": "MOCK",
        "status": run_status,
        "resource_type": "model",
        "execution_time": model_run_duration,
        "execute_started_at": generated_at,
        "execute_completed_at": generated_at,
        "compile_started_at": generated_at,
        "compile_completed_at": generated_at,
        "rows_affected": data_injection.random_number(),
        "full_refresh": false,
        "compiled_code": "MOCK",
        "failures": 0,
        "query_id": data_injection.generate_uuid(),
        "thread_id": data_injection.generate_uuid(),
        "materialization": materialization,
        "adapter_response": "MOCK"
    } %}
    {% do elementary.insert_rows(relation, [model_run], true) %}
{% endmacro %}
