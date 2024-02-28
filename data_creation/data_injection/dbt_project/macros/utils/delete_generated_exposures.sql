{% macro delete_generated_exposures() %}
    {% set generated_exposure_query %}
    select distinct unique_id
    from {{ source('elementary', 'elementary_exposures') }}
    where meta ilike '%injected_exposure%'
    {% endset %}

    {% set result = run_query(generated_exposure_query) %}
    {% for row in result %}
        {% set unique_id = row['unique_id'] %}
        {% do data_injection.delete_exposure_data(unique_id) %}
    {% endfor %}
{% endmacro %}

{% macro delete_exposure_data(unique_id) %}
  {% set delete_elementary_exposures_rows_query %}
        delete from {{ source('elementary', 'elementary_exposures') }}
        where unique_id = '{{ unique_id }}'
    {% endset %}
    {% do run_query(delete_elementary_exposures_rows_query) %}
{% endmacro %}
