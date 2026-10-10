-- How many times each source picked each number from 1 to 100, and what share
-- of that source's picks it was.
--
-- Every number gets a row for every source, 300 rows in all. A number a
-- source never picked is a real 0% share, and the distance measures in the
-- marts need that zero to be there.
WITH
    -- One row is one pick. Staging tests that each source's key is unique.
    UnionSources AS (
        SELECT
            number_chosen,
            'ai' AS source,
            MAX(dl_date) AS dl_date,
            COUNT(*) AS choice_count
        FROM {{ ref('stg_random_num_ai') }}
        GROUP BY 1, 2
        UNION ALL
        SELECT
            number_chosen,
            'people' AS source,
            MAX(dl_date) AS dl_date,
            COUNT(*) AS choice_count
        FROM {{ ref('stg_random_num_people') }}
        GROUP BY 1, 2
        UNION ALL
        SELECT
            number_chosen,
            'computer' AS source,
            MAX(dl_date) AS dl_date,
            COUNT(*) AS choice_count
        FROM {{ ref('stg_random_num_true') }}
        GROUP BY 1, 2
    ),
    SourceTotals AS (
        SELECT
            source,
            MAX(dl_date) AS dl_date,
            CAST(SUM(choice_count) AS BIGINT) AS total_choice_count
        FROM UnionSources
        GROUP BY 1
    ),
    NumSpine AS (
        SELECT
            num AS number_chosen
        FROM generate_series(1, 100) AS t(num)
    ),
    -- Numbers crossed with sources, so a number a source never picked still
    -- has a row, with a count of 0.
    UnionSpine AS (
        SELECT
            N.number_chosen,
            S.source,
            S.dl_date,
            S.total_choice_count,
            COALESCE(U.choice_count, 0) AS choice_count
        FROM NumSpine AS N
        CROSS JOIN SourceTotals AS S
        LEFT JOIN UnionSources AS U
            ON  U.number_chosen = N.number_chosen
            AND U.source = S.source
    )
SELECT
    number_chosen,
    -- '40s' is 40 to 49. '0s' is 1 to 9 and '100s' is 100 alone.
    CONCAT(CAST((number_chosen // 10) * 10 AS VARCHAR), 's') AS number_tens_range,
    (number_chosen % 2 = 0) AS number_is_even,
    (number_chosen % 2 != 0) AS number_is_odd,
    number_chosen % 10 AS number_last_digit,
    source,
    dl_date,
    -- DuckDB's / is decimal division, also between two integers
    (choice_count / total_choice_count) * 100 AS choice_count_share,
    choice_count,
    total_choice_count AS total_source_choice_count
FROM UnionSpine
