-- The category vocabulary that feeds the clustering step.
--
-- Flattens the three list columns worth clustering (sub_category, mechanism
-- and the 'Theme:' part of family) into one list of distinct terms with a
-- game count for each, and turns each term into a key with format_category().
-- The keys are exported, embedded with a sentence transformer and clustered
-- outside dbt. The clusters come back as the IN lists in
-- int_board_games_categories, which builds its keys with the same macro.
--
-- (category_raw, category_type) leads back to the source list, so a cluster
-- assignment can be traced to the exact BGG term.
WITH
    RawUnion AS (
        SELECT
            sub_category AS category_raw,
            sub_category AS category_formatted,
            'sub_category' AS category_type,
            COUNT(DISTINCT board_game_url) AS game_count
        FROM {{ ref('stg_board_games') }}, UNNEST(sub_category_list) AS sub_category
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT
            family AS category_raw,
            -- the 'Theme: ' prefix comes off here, before format_category()
            REGEXP_REPLACE(family, '(^.*:)', '') AS category_formatted,
            CONCAT(
                'family_', 
                LOWER(TRIM(REGEXP_REPLACE(family, '(:.*$)', '')))) AS category_type, 
            COUNT(DISTINCT board_game_url) AS game_count
        FROM {{ ref('stg_board_games') }}, UNNEST(family_list) AS family
        WHERE LOWER(TRIM(REGEXP_REPLACE(family, '(:.*$)', ''))) IN (
            'theme'
           )
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT
            mechanism AS category_raw,
            mechanism AS category_formatted,
            'mechanism' AS category_type,
            COUNT(DISTINCT board_game_url) AS game_count
        FROM {{ ref('stg_board_games') }}, UNNEST(mechanism_list) AS mechanism
        GROUP BY 1, 2, 3
    )
SELECT
    category_raw,
    {{ format_category('category_formatted') }} AS category_formatted,
    category_type,
    game_count
FROM RawUnion
