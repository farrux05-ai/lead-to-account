-- ============================================================
-- Macro: generate_surrogate_key
-- Wraps dbt_utils.generate_surrogate_key for consistency.
-- Usage: {{ generate_surrogate_key(['col1', 'col2']) }}
-- ============================================================

{% macro generate_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
