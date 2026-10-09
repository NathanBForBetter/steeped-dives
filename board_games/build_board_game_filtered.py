"""Build board_game_filtered, the table the dbt project reads as a source.

board_game_raw is a point-in-time BoardGameGeek snapshot. Games added to BGG
after that snapshot are swept into board_game_discovered by the extractor
(which lives outside this repo - see the README), so this folds both together
into one de-duplicated list of non-expansion board games.

    python board_games/build_board_game_filtered.py
"""
# `steeped_dives` lives at the repo root, but running this file as a script
# puts only its own directory on sys.path - never the repo root - so the
# import below would raise ModuleNotFoundError. Adding the root explicitly
# keeps every script runnable straight from a clone; it is a no-op once the
# package is installed with `pip install -e .`.
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import logging

import psycopg2

from steeped_dives.config import postgres_kwargs

logger = logging.getLogger(__name__)

# board_game_raw is a point-in-time snapshot. Games added to BGG since then
# live in board_game_discovered, filled by the discovery sweep, and are folded
# in here so a rebuild does not throw them away.
ENSURE_DISCOVERED_TABLE = """
CREATE TABLE IF NOT EXISTS board_game_discovered (
    objectid        BIGINT PRIMARY KEY,
    board_game_url  TEXT,
    board_game_name TEXT,
    year_made       TEXT,
    subtype         TEXT,
    itemstate       TEXT,
    discovered_at   TIMESTAMPTZ DEFAULT now(),
    last_checked    TIMESTAMPTZ DEFAULT now()
);
"""

CREATE_FILTERED_TABLE = """
CREATE TABLE board_game_filtered AS
WITH from_raw AS (
    SELECT
        "primary" AS board_game_name,
        primary_href AS board_game_url,
        MAX(geek_rating::float) AS geek_rating,
        MAX(avg_rating::float) AS avg_rating,
        MAX(voter_count::float) AS voter_count,
        MAX(collection_rank::float) AS collection_rank,
        MAX(REPLACE(REPLACE(year_made, ')', ''), '(', '')) AS year_made
    FROM board_game_raw
    WHERE primary_href !~ '/boardgameexpansion/'
    GROUP BY 1, 2
),
from_discovered AS (
    -- DISTINCT ON because BGG merges duplicate submissions, so several
    -- swept objectids can resolve to one canonical URL.
    SELECT DISTINCT ON (d.board_game_url)
        d.board_game_name,
        d.board_game_url,
        NULL::float AS geek_rating,
        NULL::float AS avg_rating,
        NULL::float AS voter_count,
        NULL::float AS collection_rank,
        d.year_made
    FROM board_game_discovered d
    WHERE d.subtype = 'boardgame'
      AND d.itemstate = 'approved'
      AND strpos(d.board_game_url, '/boardgame/') > 0
      AND NOT EXISTS (
          SELECT 1 FROM from_raw r
          WHERE r.board_game_url = d.board_game_url
      )
    ORDER BY d.board_game_url, d.objectid
)
SELECT * FROM from_raw
UNION ALL
SELECT * FROM from_discovered;
"""

# geek_rating IS NULL is not a discovery marker: plenty of unranked games in
# board_game_raw have no rating either. Check provenance directly.
PROVENANCE_COUNTS = """
SELECT count(*) FILTER (
           WHERE EXISTS (SELECT 1 FROM board_game_discovered d
                         WHERE d.board_game_url = f.board_game_url)
             AND NOT EXISTS (SELECT 1 FROM board_game_raw r
                             WHERE r.primary_href = f.board_game_url)
       ),
       count(*)
FROM board_game_filtered f;
"""


def build_filtered_table():
    with psycopg2.connect(**postgres_kwargs()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(ENSURE_DISCOVERED_TABLE)
            cursor.execute("DROP TABLE IF EXISTS board_game_filtered;")
            cursor.execute(CREATE_FILTERED_TABLE)
            connection.commit()

            cursor.execute(PROVENANCE_COUNTS)
            from_discovery, total = cursor.fetchone()

    logger.info(
        f"created table board_game_filtered! {total:,} rows "
        f"({from_discovery:,} from the discovery sweep)"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    build_filtered_table()
