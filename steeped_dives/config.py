"""Single entry point for local configuration.

Every path and credential the three pipelines need lives in one .env at the
repo root. This module finds that file no matter which directory a script was
launched from, loads it once, and hands values back with a loud failure when
one is missing - so a typo surfaces at startup instead of as an empty Spark
read or a silently skipped directory halfway through a 17M-row job.

    from steeped_dives.config import env_path, env_str, env_int, postgres_dsn

The repo ships a .env.example as the committed contract; .env itself is
gitignored.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPO_ROOT / ".env"


@lru_cache(maxsize=1)
def load_env() -> Path:
    """Load the repo-root .env exactly once. Real env vars always win.

    override=False is the important half: it lets CI, a container or a
    one-off `FLIGHTS_PARQUET_DIR=... python 02_silver_staging.py` take
    precedence over the file without editing it.
    """
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"no .env at {ENV_FILE} - copy .env.example to .env and fill it in"
        )
    load_dotenv(ENV_FILE, override=False)
    return ENV_FILE


def env_str(name: str, default: str | None = None) -> str:
    """Return a required string, or `default` when one is given."""
    load_env()
    value = os.getenv(name, default)
    if value is None or value == "":
        raise KeyError(
            f"{name} is not set - add it to {ENV_FILE} (see .env.example)"
        )
    return value


def env_int(name: str, default: int | None = None) -> int:
    raw = env_str(name, None if default is None else str(default))
    try:
        return int(raw)
    except ValueError as error:
        raise ValueError(f"{name}={raw!r} is not an integer") from error


def env_path(name: str, *, must_exist: bool = False) -> Path:
    """Return a path from the environment, optionally asserting it is there.

    must_exist=True on an input directory turns 'the glob matched nothing'
    into 'this directory does not exist', which is the error you actually
    wanted to read.
    """
    path = Path(env_str(name)).expanduser()
    if must_exist and not path.exists():
        raise FileNotFoundError(f"{name} points at {path}, which does not exist")
    return path


def postgres_kwargs() -> dict:
    """psycopg2.connect(**postgres_kwargs()) using the libpq variable names."""
    load_env()
    return {
        "host": env_str("PGHOST", "localhost"),
        "port": env_str("PGPORT", "5432"),
        "user": env_str("PGUSER", "postgres"),
        "password": env_str("PGPASSWORD"),
        "dbname": env_str("PGDATABASE", "postgres"),
    }


def postgres_dsn() -> str:
    """Same connection as a URI, for tools that want one string."""
    kwargs = postgres_kwargs()
    return (
        f"postgresql://{kwargs['user']}:{kwargs['password']}"
        f"@{kwargs['host']}:{kwargs['port']}/{kwargs['dbname']}"
    )
