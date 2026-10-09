-- A game has at most one open version. Two open rows would mean the snapshot
-- failed to close the old one, and any "current state" query would double.
SELECT
    board_game_url,
    COUNT(*) AS open_rows
FROM {{ ref('scd_board_game') }}
WHERE dbt_valid_to IS NULL
GROUP BY board_game_url
HAVING COUNT(*) > 1
