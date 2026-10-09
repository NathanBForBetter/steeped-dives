{#
    Turn a BGG category term into a lookup key.

        1. lowercase
        2. ' / '               -> ' or '    alternatives, written out
        3. ( ) : games game    -> ''        punctuation and filler words
        4. '-'                 -> ' '       hyphens
        5. runs of spaces      -> ' '
        6. trim both ends

    'Card Game' becomes 'card', 'Trick-taking' becomes 'trick taking' and
    'Game System' becomes 'system'. Possessives are kept, so "Children's Game"
    becomes "children's".

    Lowercasing comes first because step 3 matches lowercase 'game' only.

    Both int_board_games_category_vocabulary and int_board_games_categories
    build their keys with this macro. The IN lists in the categories model
    hold clusters of the vocabulary's keys, so the two models have to produce
    the same string for the same term.

    Step 3 removes ':', so a prefix such as 'Theme: ' has to be stripped
    before the term is passed in.
#}
{% macro format_category(column_name) %}

    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER({{ column_name }}),
                    ' / ', ' or ', 'g'
                ),
                '\(|\)|\:|games|game', '', 'g'
            ),
            '-', ' ', 'g'
        ),
        ' +', ' ', 'g'
    ))

{% endmacro %}
