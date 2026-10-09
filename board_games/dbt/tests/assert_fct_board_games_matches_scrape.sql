-- fct_board_games reads its descriptive attributes through the snapshot, using
-- the version in force on scrape_date. For the scrape being built, that
-- version has to say exactly what the scrape said.
--
-- This fails when the join finds no version (attributes come back null) or a
-- stale one (the models were rebuilt after a new scrape without the snapshot
-- being run). Neither breaks the grain, so the grain test cannot see them.
SELECT
    I.board_game_url
FROM {{ ref('int_board_games_main_dims') }} AS I
LEFT JOIN {{ ref('fct_board_games') }} AS F
    ON F.board_game_url = I.board_game_url
WHERE F.board_game_url IS NULL
    OR F.board_game_name IS DISTINCT FROM I.board_game_name
    OR F.year_made_int IS DISTINCT FROM I.year_made_int
    OR F.short_descr IS DISTINCT FROM I.short_descr
    OR F.main_publisher IS DISTINCT FROM I.main_publisher
    OR F.secondary_publisher IS DISTINCT FROM I.secondary_publisher
