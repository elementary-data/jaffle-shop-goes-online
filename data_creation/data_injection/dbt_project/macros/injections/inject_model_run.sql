{% macro inject_model_run(model_id, generated_at, run_status, materialization, run_duration = none) %}
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

{%- macro get_models_runs(days_back = 7, exclude_elementary=false) -%}
    {% set models_runs_query %}
        with model_runs as (
            select * from {{ ref('elementary', 'model_run_results') }}
        )

        select
            unique_id, 
            invocation_id,
            name,
            schema_name as schema,
            status,
            case
                when status != 'success' then 0
                else round({{ elementary.edr_cast_as_numeric('execution_time') }}, 1)
            end as execution_time,
            full_refresh,
            materialization,
            compiled_code,
            generated_at
        from model_runs
        where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('generated_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
        {% if exclude_elementary %}
          and unique_id not like 'model.elementary.%'
        {% endif %}
        order by generated_at
    {% endset %}
    {% set models_runs_agate = run_query(models_runs_query) %}
    {% do return(elementary.agate_to_dicts(models_runs_agate)) %}
{%- endmacro -%}
