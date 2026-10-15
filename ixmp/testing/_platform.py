import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from ixmp import Platform
from ixmp import config as ixmp_config
from ixmp.util.ixmp4 import is_ixmp4backend

from .postgres import TempDatabaseFactory, as_database_name

if TYPE_CHECKING:
    from . import BackendDriver

    ArgsKwargs = tuple[list[str], dict[str, Any]]
    PlatformFixtureSubtype = type["IXMP4PostgreSQL | JDBCHyperSQL | JDBCPostgreSQL"]


@dataclass
class PlatformFixture(ABC):
    factory: "TempPlatformFactory"
    name: str
    backend: str
    scope: str

    @abstractmethod
    def get_args(self) -> "ArgsKwargs": ...

    def __enter__(self) -> Platform:
        # Add platform to ixmp configuration
        args, kwargs = self.get_args()
        ixmp_config.add_platform(self.name, self.backend, *args, **kwargs)

        # Launch Platform
        self.mp = Platform(name=self.name)

        return self.mp

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        # Teardown: don't show log messages when destroying the platform, even if the
        # test using the fixture modified the log level
        self.mp._backend.set_log_level(logging.CRITICAL)

        del self.mp

    def __del__(self) -> None:
        try:
            # Remove from configuration
            ixmp_config.remove_platform(self.name)
        except KeyError:
            pass


@dataclass
class IXMP4PostgreSQL(PlatformFixture):
    """:class:`.IXMP4Backend` with a PostgreSQL database."""

    def get_args(self) -> "ArgsKwargs":
        # Silence noisy debug logging from ixmp4 → litestar → polyfactory → faker
        logging.getLogger("faker").setLevel(logging.INFO)

        # Format the name for the ixmp4.Platform. IXMP4Backend requires that the
        # database name and ixmp4 configuration name match.
        ixmp4_name = as_database_name(self.name, max_length=60)

        # Create a database with exactly this name
        dsn = self.factory.postgres_db_factory.make_database(ixmp4_name)
        assert dsn, self.factory.postgres_db_factory.messages

        return [], dict(ixmp4_name=ixmp4_name, dsn=dsn, jdbc_compat=True)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        self.mp._backend.set_log_level(logging.CRITICAL)

        # NOTE Following the teardown in ixmp4's backend fixtures. Due to the setup
        # above, mp._backend._backend is always of type PostgresTestBackend
        assert is_ixmp4backend(self.mp._backend)
        from ixmp4.transport import DirectTransport

        transport = self.mp._backend._backend.transport
        if isinstance(transport, DirectTransport):
            transport.close()

        del self.mp

        # Remove from configuration
        ixmp_config.remove_platform(self.name)


class JDBCHyperSQL(PlatformFixture):
    """:class:`.JDBCBackend` with an in-memory, HyperSQL database."""

    def get_args(self) -> "ArgsKwargs":
        return ["hsqldb"], dict(url=f"jdbc:hsqldb:mem:{as_database_name(self.name)}")


class JDBCPostgreSQL(PlatformFixture):
    """:class:`.JDBCBackend` with a PostgreSQL database."""

    def get_args(self) -> "ArgsKwargs":
        # Create a new database
        db_name = as_database_name(self.name)

        return ["postgresql"], dict(
            url=self.factory.postgres_db_factory.make_database(db_name)
        )


#: Mapping from (backend, driver) to PlatformFixture subclasses.
FIXTURE_CLASS: dict[tuple[str, str], "PlatformFixtureSubtype"] = {
    ("ixmp4", "postgresql"): IXMP4PostgreSQL,
    ("jdbc", "hsqldb"): JDBCHyperSQL,
    ("jdbc", "postgresql"): JDBCPostgreSQL,
}


@dataclass
class TempPlatformFactory:
    """Factory for :class:`.Platform` used as fixtures."""

    stash: pytest.Stash
    postgres_db_factory: TempDatabaseFactory

    def make_platform(
        self,
        request: pytest.FixtureRequest,
        backend_driver: "BackendDriver",
        *,
        name: str = "",
    ) -> PlatformFixture:
        # Long, unique name for the platform.
        # - Use the pytest.Node ID.
        # - Remove '/' so that the name can be used in URL tests.
        # - Prepend the backend and driver.
        node_id = re.sub(rf"\.py|{backend_driver}", "", request.node.nodeid)
        node_id = re.sub(r"[./\s:\[\]-]+", "_", node_id).strip("_")
        platform_name = name or " ".join([str(backend_driver), node_id])

        # Identify a class to prepare the fixture
        cls = FIXTURE_CLASS[backend_driver]

        # Create a platform fixture and enter its context.
        # This sets up any associated database(s) and adds to config.
        return cls(self, platform_name, backend_driver[0], request.scope)
