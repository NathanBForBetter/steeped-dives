{#
    Asserts a table has exactly one row per the given column list.

    The same test exists in the natality project under the same name, so
    "what is the grain of this model, and is it actually that" is answered the
    same way across both warehouses. A `unique` test on a single column cannot
    express a composite grain, and dbt_utils.unique_combination_of_columns
    does not report the offending values - this returns them, so a failure
    tells you which group fanned out rather than just that one did.

        data_tests:
          - assert_grain:
              arguments:
                columns: [board_game_url]
#}
{% test assert_grain(model, columns) %}

SELECT
    {{ columns | join(', ') }},
    COUNT(*) AS row_count
FROM {{ model }}
GROUP BY {{ columns | join(', ') }}
HAVING COUNT(*) > 1

{% endtest %}
