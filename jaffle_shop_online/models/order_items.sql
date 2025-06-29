-- depends_on: {{ ref('total_orders') }}

{% if execute %}
  {% set random_bool = run_query('select random() % 2 = 0')[0].values()[0] %}
  {% if random_bool %}
    select * from {{ ref('total_orders') }}
  {% else %}
    select * from {{ ref('total_orders') }}
  {% endif %}
{% endif %}
