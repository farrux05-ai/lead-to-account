-- ============================================================
-- Macro: safe_divide
-- Returns NULL instead of division-by-zero errors.
-- Usage: {{ safe_divide('numerator', 'denominator') }}
-- ============================================================

{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
        THEN NULL
        ELSE CAST({{ numerator }} AS DOUBLE) / NULLIF(CAST({{ denominator }} AS DOUBLE), 0.0)
    END
{% endmacro %}
