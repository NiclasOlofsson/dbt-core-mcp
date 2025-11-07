{% macro execute_sql(sql) %}
  {% set results = run_query(sql) %}
  {% if execute %}
    {{ log(results, info=True) }}
    {{ return(results) }}
  {% endif %}
{% endmacro %}
