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
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.dataset as ds

from steeped_dives.config import env_path

logger = logging.getLogger(__name__)


def convert_zip_directory_to_partitioned_parquet(
    zip_dir_path: str,
    output_dir: str,
    date_column: str = "FL_DATE",
    # BTS ships FL_DATE as a 12-hour timestamp at midnight, e.g. "1/1/2024 12:00:00 AM"
    date_format: str = "%m/%d/%Y %I:%M:%S %p",
    num_salts: int = 4,
    delete_zips_after: bool = False,
):
    """Processes ALL BTS flight .zip files in a directory, extracts CSVs in-memory,
    and writes them into a unified Hive-partitioned Parquet dataset.
    """
    zip_dir = Path(zip_dir_path)
    output_path = Path(output_dir)

    zip_files = list(zip_dir.glob("*.zip"))

    if not zip_files:
        logger.warning(f"No .zip files found in directory: {zip_dir}")
        return

    logger.info(
        f"Found {len(zip_files)} ZIP file(s) to process in '{zip_dir}'"
    )

    failures: list[tuple[str, str]] = []
    written_rows = 0

    for zip_path in zip_files:
        logger.info(f"Processing archive: {zip_path.name}")

        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                csv_names = [f for f in z.namelist() if f.endswith(".csv")]

                if not csv_names:
                    logger.warning(
                        f"No CSV files found inside archive: {zip_path.name}"
                    )
                    continue

                for csv_name in csv_names:
                    logger.info(
                        f"Reading internal CSV: {csv_name} from {zip_path.name}"
                    )

                    with z.open(csv_name) as csv_file:
                        # 1. Read CSV into PyArrow Table
                        table = pv.read_csv(csv_file)

                        # 2. Parse Timestamp
                        raw_dates = table[date_column]
                        timestamps = pc.strptime(
                            raw_dates,
                            format=date_format,
                            unit="s",
                            error_is_null=True,
                        )

                        # Total parse failure means the wrong format string.
                        parsed = pc.count(timestamps).as_py()
                        if parsed == 0:
                            raise ValueError(
                                f"Failed to parse any valid dates from column '{date_column}' "
                                f"using format '{date_format}'. Sample raw value: {raw_dates[0]}"
                            )

                        # A *partial* failure is the quiet one: pc.year(null)
                        # is null, so those rows would be filed under
                        # __HIVE_DEFAULT_PARTITION__ rather than year=/month=
                        # and drop out of any partition-pruned read. Refuse
                        # to write them instead.
                        unparsed = len(table) - parsed
                        if unparsed:
                            raise ValueError(
                                f"{unparsed:,} of {len(table):,} rows in {csv_name} have a "
                                f"'{date_column}' that does not match '{date_format}'; "
                                f"they would be written to an unpartitioned directory. "
                                f"Sample raw value: {raw_dates[0]}"
                            )

                        # 3. Clean up existing BTS date columns before adding partition columns
                        # BTS CSVs include uppercase 'YEAR' and 'MONTH' columns by default
                        cols_to_drop = [
                            col
                            for col in ["YEAR", "MONTH", "year", "month"]
                            if col in table.column_names
                        ]
                        if cols_to_drop:
                            table = table.drop(cols_to_drop)

                        # Extract clean partition columns
                        table = table.append_column("year", pc.year(timestamps))
                        table = table.append_column(
                            "month", pc.strftime(timestamps, format="%m")
                        )

                        # 4. Add Salt Column for File Distribution
                        num_rows = len(table)
                        salt_values = [i % num_salts for i in range(num_rows)]
                        table = table.append_column("salt", pa.array(salt_values))
                        written_rows += num_rows

                        # 5. Write Partitioned Parquet Dataset
                        ds.write_dataset(
                            data=table,
                            base_dir=output_path,
                            format="parquet",
                            partitioning=ds.partitioning(
                                pa.schema([
                                ("year", pa.int64()),
                                ("month", pa.string()),
                                ("salt", pa.int64()),
                                ]),
                                flavor="hive",
                            ),
                            existing_data_behavior="delete_matching",
                        )

            if delete_zips_after:
                logger.info(f"Deleting processed archive: {zip_path.name}")
                zip_path.unlink()

        except Exception as error:
            # Keep going through the remaining archives, but remember this one:
            # a run that lost a month of flights must not report success.
            failures.append((zip_path.name, str(error)))
            logger.error(
                f"Failed to process archive {zip_path.name}: {error}",
                exc_info=True,
            )
            logger.info("Skipping deletion due to error.")

    logger.info(
        f"Wrote {written_rows:,} row(s) from {len(zip_files) - len(failures)} "
        f"of {len(zip_files)} archive(s) to: {output_path}"
    )
    if failures:
        detail = "; ".join(f"{name}: {reason}" for name, reason in failures)
        raise RuntimeError(
            f"{len(failures)} of {len(zip_files)} archive(s) failed - {detail}"
        )


# --- Entry point: BTS On-Time Performance zips -> bronze Parquet ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    convert_zip_directory_to_partitioned_parquet(
        zip_dir_path=str(env_path("FLIGHTS_ZIP_DIR", must_exist=True)),
        output_dir=str(env_path("FLIGHTS_PARQUET_DIR")),
        date_column="FL_DATE",
        num_salts=4,
    )