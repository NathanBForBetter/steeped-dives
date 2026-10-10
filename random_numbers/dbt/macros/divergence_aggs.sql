{#
    Distance measures between two distributions over the numbers 1 to 100.

    Both inputs are shares in percent, one row per number, and each has to add
    up to 100 over the rows being aggregated.

        total_abs_diff    sum of |p - q|, in share points (0 to 200)
        divergence_perc   total variation distance as a percent (0 to 100)
        js_divergence     Jensen-Shannon divergence, base 2 (0 to 1)
        avg_abs_diff      mean |p - q| per number
        max_abs_diff      largest |p - q| for a single number

    Each helper wraps what it returns in parentheses, so it can sit inside a
    larger expression such as a / b without changing the order of operations.
#}

{#
    Kullback-Leibler divergence of a from b, base 2, summed over the group.
    A row where a is 0 adds nothing. b has to be above 0 wherever a is, which
    always holds for the mixture m_prob() returns.
#}
{% macro kl_diverge(a, b) %}
    SUM(
        CASE
            WHEN {{ a }} > 0
                THEN {{ a }} * LOG2({{ a }} / {{ b }})
            ELSE 0
        END
    )
{% endmacro %}


{# A share in percent as a probability. #}
{% macro div_100(input) %}
    ({{ input }} / 100.0)
{% endmacro %}


{# The midpoint of two distributions, as a probability. #}
{% macro m_prob(share_1, share_2) %}
    (0.5 * (
        {{ div_100(share_1) }}
        + {{ div_100(share_2) }}
    ))
{% endmacro %}


{% macro divergence_aggs(share_1, share_2) %}
    SUM(ABS({{ share_1 }} - {{ share_2 }})) AS total_abs_diff,
    SUM(ABS({{ share_1 }} - {{ share_2 }})) / 2.0 AS divergence_perc,

    0.5 * {{ kl_diverge(
        div_100(share_1),
        m_prob(share_1, share_2)
    ) }}
    + 0.5 * {{ kl_diverge(
        div_100(share_2),
        m_prob(share_1, share_2)
    ) }} AS js_divergence,

    AVG(ABS({{ share_1 }} - {{ share_2 }})) AS avg_abs_diff,
    MAX(ABS({{ share_1 }} - {{ share_2 }})) AS max_abs_diff
{% endmacro %}
