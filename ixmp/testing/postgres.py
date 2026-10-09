"""Test utilities for working with PostgreSQL databases."""

import logging

import pytest
from xdist import get_xdist_worker_id

from ixmp.util.ixmp4 import format_url

log = logging.getLogger(__name__)


def databases_for_worker(session: pytest.Session, create: bool = True) -> None:
    """Set up test databases on a PostgreSQL server for the test session."""
    from . import KEY_BACKEND_DRIVER, KEY_POSTGRES_DB

    stash = session.config.stash
    result = stash.get(KEY_POSTGRES_DB, {})
    for bd in session.config.stash[KEY_BACKEND_DRIVER]:
        if bd.driver == "postgresql":
            result.setdefault(bd, "")
    stash[KEY_POSTGRES_DB] = result

    # Silence noisy debug logging from ixmp4 → litestar → polyfactory → faker
    logging.getLogger("faker").setLevel(logging.INFO)

    # Connect to a PostgreSQL server and create a test database for this worker

    worker_id = get_xdist_worker_id(session)
    url = session.config.option.ixmp_postgres.rpartition("/")[0]

    try:
        import psycopg

        with psycopg.connect(url, autocommit=True) as connection:
            for bd in result.keys():
                # Format test database name
                db_name = f"test_ixmp_{worker_id}_{bd.backend}"
                url = format_url(url, database=db_name)

                # Clear connections
                connection.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                # Drop the existing database
                connection.execute(f"DROP DATABASE IF EXISTS {db_name}")

                if create:
                    # Recreate the database
                    connection.execute(f"CREATE DATABASE {db_name}")

                    if bd.backend == "ixmp4":
                        # Initialize schema on the new DB so ixmp4 can use it
                        from ixmp4.db import get_alembic_controller

                        get_alembic_controller(url).upgrade_database("head")

                result[bd] = url
    except Exception as e:  # pragma: no cover
        # Some error connecting to the database → store a message and text of `e`
        log.error(f"Exception: {e!r}\nURL: {url}")
