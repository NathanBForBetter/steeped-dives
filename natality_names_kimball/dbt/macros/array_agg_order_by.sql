{#
    Spark has no ARRAY_AGG(x ORDER BY y), so this is the standard workaround:
    collect (sort_key, value) structs, then sort_array them, which orders by
    the struct's first field.

    The field names are aliased explicitly rather than inherited from the
    expressions, so the caller always reads `.sort_key` and `.value` no matter
    what columns were passed in - element [0] of the result is a STRUCT, not
    the value, and indexing it without the field name silently ships a struct
    downstream.

        {{ array_agg_order_by('origin', 'name_count', ascending=False) }}
        -> sort_array(collect_list(struct(name_count AS sort_key,
                                          origin    AS value)), FALSE)

        ... origins_ordered[0].value AS main_origin

    ascending=False puts the largest sort_key at [0].
#}
{% macro array_agg_order_by(value_field, sort_field, ascending=true) %}

SORT_ARRAY(
    COLLECT_LIST(
        STRUCT(
            {{ sort_field }} AS sort_key,
            {{ value_field }} AS value
        )
    ),
    {{ 'TRUE' if ascending else 'FALSE' }}
)

{% endmacro %}
