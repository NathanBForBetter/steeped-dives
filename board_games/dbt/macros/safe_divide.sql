{#
    Division that returns NULL instead of erroring or returning Infinity when
    the denominator is 0 or NULL.

    Written with adapter.dispatch because this repo runs two warehouses: the
    board_games project is Postgres, which raises division_by_zero, and the
    natality project is Spark, which has a native try_divide. One macro name,
    one call site, correct behavior on both.

        {{ safe_divide('delay_minutes', 'flight_count') }}

    Both arguments are cast to a float type first, so integer division never
    silently truncates a share to 0 or 1.
#}
{% macro safe_divide(numerator, denominator) -%}
    {{ return(adapter.dispatch('safe_divide', 'steeped_macros')(numerator, denominator)) }}
{%- endmacro %}


{% macro default__safe_divide(numerator, denominator) -%}
    ({{ numerator }}) * 1.0 / NULLIF(({{ denominator }}), 0)
{%- endmacro %}


{% macro postgres__safe_divide(numerator, denominator) -%}
    ({{ numerator }})::numeric / NULLIF(({{ denominator }}), 0)
{%- endmacro %}


{% macro spark__safe_divide(numerator, denominator) -%}
    TRY_DIVIDE(CAST(({{ numerator }}) AS DOUBLE), CAST(({{ denominator }}) AS DOUBLE))
{%- endmacro %}
