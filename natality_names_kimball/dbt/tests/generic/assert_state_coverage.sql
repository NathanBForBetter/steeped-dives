{#
    Asserts the dimension covers all 50 states plus DC and nothing else.

    A completeness test, which `not_null` and `unique` cannot express: both
    pass happily on a dimension that lost half its members because an
    upstream filter changed. 51 is the number both sources agree on - SSA
    ships one file per state plus DC, and the CDC extract is pulled by state
    of residence over the same set.
#}
{% test assert_state_coverage(model, state_column, expected_count=51) %}

SELECT
    COUNT(DISTINCT {{ state_column }}) AS state_count
FROM {{ model }}
HAVING COUNT(DISTINCT {{ state_column }}) != {{ expected_count }}

{% endtest %}
