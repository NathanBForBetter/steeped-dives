"""Data tests for the Spark side of the flights pipeline.

The dbt projects in this repo get `unique`, `not_null`, `relationships`,
`accepted_values` and a custom `assert_grain` for free. This pipeline has no
dbt, so the same five assertions are implemented here and called from the
transform scripts and from 06_run_data_tests.py.

Each one raises on failure rather than logging, so a pipeline run cannot
quietly continue past a broken grain and write a wrong mart.
"""
import logging

logger = logging.getLogger(__name__)


def test_unique(spark, table, column):
    """No value of `column` appears in more than one row."""
    query = f"""
        SELECT {column}, COUNT(*) AS n
        FROM {table}
        GROUP BY {column}
        HAVING COUNT(*) > 1
    """

    failures = spark.sql(query).count()

    if failures:
        raise ValueError(
            f"{table}.{column} is not unique - {failures:,} duplicated value(s)"
        )
    logger.info(f"PASS unique: {table}.{column}")


def test_not_null(spark, table, column):
    """`column` holds no nulls."""
    query = f"""
        SELECT COUNT(*) AS null_count
        FROM {table}
        WHERE {column} IS NULL
    """

    null_count = spark.sql(query).first()["null_count"]

    if null_count:
        raise ValueError(
            f"{table}.{column} contains {null_count:,} null values"
        )
    logger.info(f"PASS not_null: {table}.{column}")


def test_assert_grain(spark, table, *grain_columns):
    """Exactly one row per the given column combination.

    grain_columns arrives as a tuple, so both of these work:
        test_assert_grain(spark, 'fct_flights', 'flight_id')
        test_assert_grain(spark, 'fct_flights', 'route_key', 'date_key')
    """
    cols_str = ", ".join(grain_columns)
    query = f"""
        SELECT
            {cols_str},
            count(*) AS rows_for_grain
        FROM {table}
        GROUP BY {cols_str}
        HAVING count(*) > 1
    """

    first_row = spark.sql(query).first()

    # Check if a row was returned before indexing
    if first_row is not None:
        rows_for_grain = first_row["rows_for_grain"]
        raise ValueError(
            f"{table} does not have the expected grain for columns: {cols_str} "
            f"(found groups with up to {rows_for_grain} duplicate rows)"
        )
    logger.info(f"PASS grain: {table} ({cols_str})")


def test_relationship(spark, table, column, to_table, to_column):
    """Every non-null value of `column` exists in `to_table.to_column`.

    This is the one a `unique` test on the dimension cannot give you: it
    catches a fact row whose foreign key no dimension row answers to, which
    is how a star quietly starts losing rows in every dimensional rollup.
    Nulls are skipped, matching dbt's `relationships` test.
    """
    query = f"""
        SELECT COUNT(*) AS orphan_count
        FROM {table} AS child
        LEFT JOIN {to_table} AS parent
            ON child.{column} = parent.{to_column}
        WHERE child.{column} IS NOT NULL
          AND parent.{to_column} IS NULL
    """

    orphans = spark.sql(query).first()["orphan_count"]

    if orphans:
        raise ValueError(
            f"{table}.{column} has {orphans:,} value(s) with no matching "
            f"{to_table}.{to_column}"
        )
    logger.info(f"PASS relationships: {table}.{column} -> {to_table}.{to_column}")


def test_rows_are_real(spark, table, source_table, key, columns):
    """Every row in `table` exists, on `columns`, in `source_table`.

    This checks that a de-duplication picked one whole row. Resolving a
    collision column by column, with a mix of ANY_VALUE and MAX, can put an
    arr_delay from one source row beside a dep_delay from the other and
    produce a flight that was never reported. A grain test passes on a row
    like that: the key is unique and no column is null.

    Null-safe (<=>) throughout, because most of these columns are null on a
    cancelled flight and `=` would report every one of them as fabricated.
    """
    on_clause = "\n          AND ".join(
        [f"child.{key} = parent.{key}"]
        + [f"child.{c} <=> parent.{c}" for c in columns]
    )
    query = f"""
        SELECT COUNT(*) AS fabricated_rows
        FROM {table} AS child
        LEFT ANTI JOIN {source_table} AS parent
            ON {on_clause}
    """

    fabricated = spark.sql(query).first()["fabricated_rows"]

    if fabricated:
        raise ValueError(
            f"{table} holds {fabricated:,} row(s) that do not exist in "
            f"{source_table} on ({', '.join(columns)}) - the de-duplication "
            f"is assembling rows from more than one source row"
        )
    logger.info(f"PASS rows_are_real: {table} -> {source_table}")


def test_accepted_values(spark, table, column, values):
    """`column` holds nothing outside `values` (nulls are allowed)."""
    quoted = ", ".join(f"'{value}'" for value in values)
    query = f"""
        SELECT {column}, COUNT(*) AS n
        FROM {table}
        WHERE {column} IS NOT NULL
          AND {column} NOT IN ({quoted})
        GROUP BY {column}
    """

    rows = spark.sql(query).collect()

    if rows:
        found = ", ".join(f"{row[column]!r} ({row['n']:,})" for row in rows)
        raise ValueError(
            f"{table}.{column} holds unexpected value(s): {found}"
        )
    logger.info(f"PASS accepted_values: {table}.{column}")
