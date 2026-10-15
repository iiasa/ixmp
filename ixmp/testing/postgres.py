"""Test utilities for working with PostgreSQL databases."""

import logging
from dataclasses import InitVar, dataclass, field
from hashlib import blake2s
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

import pytest
from xdist import get_xdist_worker_id

from ixmp.util.ixmp4 import format_url

if TYPE_CHECKING:
    from psycopg import Connection

log = logging.getLogger(__name__)


@dataclass
class TempDatabaseFactory:
    """Factory for temporary PostgreSQL databases."""

    session: InitVar[pytest.Session]
    base_url: str = ""
    cache_dir: Path = field(default_factory=Path)
    is_controller: bool = False
    messages: list[str] = field(default_factory=list)

    def __post_init__(self, session: pytest.Session) -> None:
        self.base_url = session.config.option.ixmp_postgres.rpartition("/")[0]
        self.cache_dir = session.config.cache.mkdir("postgresql")
        self.is_controller = get_xdist_worker_id(session) == "master"

    def drop_all(self) -> None:  # pragma: no cover
        """Drop all databases created by this factory."""
        if not self.is_controller:
            return
        try:
            import psycopg

            with psycopg.connect(self.base_url, autocommit=True) as connection:
                # Iterate over files in the Pytest cache directory
                for path in self.cache_dir.glob("*"):
                    # File names are database names; drop the associated database
                    clear_connections_and_drop(connection, path.name)
                    path.unlink()
        except Exception:
            pass

    def make_database(self, name: str) -> str:
        """Create a single test database."""
        url = format_url(self.base_url, database=name)

        # Check a file in the Pytest cache to determine if the database was already
        # created: for instance, by an earlier test or by another pytest-xdist worker
        cache_file = self.cache_dir.joinpath(name)
        if cache_file.exists():
            return url

        try:
            import psycopg

            # Create a file that indicates the database has been created
            cache_file.touch(exist_ok=False)

            # Recreate the database
            with psycopg.connect(self.base_url, autocommit=True) as connection:
                clear_connections_and_drop(connection, name)
                connection.execute(f"CREATE DATABASE {name}")
        except FileExistsError:  # pragma: no cover
            # Another pytest-xdist worker is in the process of creating the database
            sleep(0.5)
        except Exception as e:  # pragma: no cover
            # Some other error → store a message and text of `e`
            self.messages.append(f"Exception: {e!r}\nURL: {url}")
            url = ""

        return url


def as_database_name(value: str, max_length: int = 63) -> str:
    """Format a PostgreSQL database name from `value`.

    The returned name…

    - has spaces (" ") replaced with underscores("_").
    - is lowercase.
    - is of `max_length` or less. Longer `value`s are truncated, with the final 10
      characters replaced by a unique hash of the full `value`.
    """
    result = value.replace(" ", "_").lower()

    # PostgreSQL database names (a) cannot contain " " and (b) must be ≤63 chars.

    if len(result) <= max_length:
        return result
    else:
        digest = blake2s(value.encode()).hexdigest()
        # Construct a unique name of max_length characters
        return result[: max_length - 10] + digest[:10]


def clear_connections_and_drop(connection: "Connection", name: str) -> None:
    """Clear all connections to database `name`, then drop it."""
    connection.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND "
        "pid <> pg_backend_pid()",
        (name,),
    )
    connection.execute(f"DROP DATABASE IF EXISTS {name}")
