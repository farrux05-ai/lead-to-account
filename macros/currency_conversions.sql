-- ============================================================
-- Macro: cents_to_dollars
-- Converts integer cents/micros to decimal dollars.
-- Google Ads reports spend in micros (millionths of a dollar).
-- Meta Ads reports spend in cents.
-- Usage: {{ cents_to_dollars('spend_cents') }}
--        {{ micros_to_dollars('spend_micros') }}
-- ============================================================

{% macro cents_to_dollars(column_name) %}
    ROUND(CAST({{ column_name }} AS DOUBLE) / 100.0, 6)
{% endmacro %}

{% macro micros_to_dollars(column_name) %}
    ROUND(CAST({{ column_name }} AS DOUBLE) / 1000000.0, 6)
{% endmacro %}
