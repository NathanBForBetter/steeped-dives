-- One row per game: the descriptive attributes, plus a derived decade and a
-- deterministic top-two publisher pick.
WITH
    UnnestPub AS (
        SELECT
            publisher,
            board_game_url
        FROM {{ ref('stg_board_games') }}, UNNEST(publisher_list) AS publisher
    ),
    PublisherCount AS (
        SELECT
            publisher,
            COUNT(board_game_url) AS game_count
        FROM UnnestPub
        GROUP BY 1
    ),
    TopPublishers AS (
        SELECT
            U.board_game_url,
            -- publisher as the tiebreaker keeps the ordering stable when two
            -- publishers have the same game_count, so main_publisher does not
            -- change between runs on identical input.
            ARRAY_AGG(U.publisher ORDER BY P.game_count DESC, U.publisher) AS top_pub_list
        FROM UnnestPub AS U
        LEFT JOIN PublisherCount AS P USING(publisher)
        GROUP BY 1
    )
SELECT
    B.board_game_url,
    B.board_game_name,
    B.scrape_date,
    -- scrape_date as a timestamp. scd_board_game uses it as updated_at, and
    -- dbt wants that to be the same type as the timestamps it writes itself.
    CAST(B.scrape_date AS TIMESTAMP) AS scraped_at,

    -- BGG uses 0 for "year unknown" and carries genuine negative years for
    -- ancient games (Senet at -3000, Go at -2200). year_made_int keeps the
    -- negatives and nulls the 0 sentinel so it is never read as 1 BCE.
    B.year_made,
    NULLIF({{ mixed_int_cast('B.year_made', 'int') }}, 0) AS year_made_int,
    CASE
        WHEN NULLIF({{ mixed_int_cast('B.year_made', 'int') }}, 0) IS NULL
            THEN NULL
        ELSE CONCAT(
            FLOOR({{ mixed_int_cast('B.year_made', 'int') }}::numeric / 10) * 10,
            's')
        END AS decade,

    B.short_descr,
    -- Postgres arrays start at 1. Taking the top two publishers gives a
    -- stable pair of columns without one-hot encoding every publisher.
    P.top_pub_list[1] AS main_publisher,
    P.top_pub_list[2] AS secondary_publisher
FROM {{ ref('stg_board_games') }} AS B
LEFT JOIN TopPublishers AS P USING (board_game_url)
