{#
    Asserts the year dimension has no holes: every year between its own min
    and max is present.

    This is the test that catches a silently-dropped source file. If one SSA
    year or one WONDER extract fails to load, min and max stay put, the row
    count falls by one, and nothing else in the project notices - the facts
    just get quieter. Comparing the distinct count to (max - min + 1) turns
    that into a failure.
#}
{% test assert_contiguous_years(model, year_column_text) %}

SELECT
    MIN(CAST({{ year_column_text }} AS INT)) AS min_year,
    MAX(CAST({{ year_column_text }} AS INT)) AS max_year,
    COUNT(DISTINCT CAST({{ year_column_text }} AS INT)) AS year_count
FROM {{ model }}
HAVING COUNT(DISTINCT CAST({{ year_column_text }} AS INT))
    != MAX(CAST({{ year_column_text }} AS INT))
     - MIN(CAST({{ year_column_text }} AS INT))
     + 1

{% endtest %}
