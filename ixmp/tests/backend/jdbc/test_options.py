import re
from pathlib import Path
from typing import Any

import pytest

from ixmp.backend.jdbc.options import DRIVER, Options


class TestOptions:
    def test_default_table_types_duplicate(self, tmp_path: Path) -> None:
        """ "hsqldb.default_table_type=cached" is added by default, at most once."""
        expr = re.compile("jdbc:hsqldb:file:[^;]*;hsqldb.default_table_type=cached")

        # Using a path
        assert expr.fullmatch(Options(DRIVER.hsqldb, path=tmp_path).full_url)

        # Using a URL
        assert expr.fullmatch(Options(DRIVER.hsqldb, url="file:foo").full_url)

        # Using a URL with the parameter already added
        opt = Options(DRIVER.hsqldb, url="file:foo;hsqldb.default_table_type=cached")
        assert expr.fullmatch(opt.full_url)

        # Existing property with default value is not overridden
        opt = Options(DRIVER.hsqldb, url="file:foo;hsqldb.default_table_type=memory")
        assert not expr.fullmatch(opt.full_url) and "=cached" not in opt.full_url

    @pytest.mark.parametrize(
        "url, expected",
        (
            ("host.example.com:5432/dbname", "host.example.com:5432/dbname"),
            ("host.example.com/dbname", "host.example.com/dbname"),
            ("//host.example.com:5432/dbname", "host.example.com:5432/dbname"),
            (
                "postgresql://host.example.com:5432/dbname",
                "host.example.com:5432/dbname",
            ),
            (
                "jdbc:postgresql://host.example.com:5432/dbname",
                "host.example.com:5432/dbname",
            ),
            ("[::1]:5432/db", "[::1]:5432/db"),
            ("h1:5432,h2:5432/db", "h1:5432,h2:5432/db"),
            ("host:5432/db?sslmode=require", "host:5432/db?sslmode=require"),
        ),
    )
    def test_postgresql_full_url(self, url: str, expected: str) -> None:
        """Partial and complete PostgreSQL URLs give the same full JDBC URL."""
        opt = Options(DRIVER.postgresql, url=url, user="USER", password="PASSWORD")
        assert f"jdbc:postgresql://{expected}" == opt.full_url

    @pytest.mark.parametrize(
        "url",
        (
            "postgres://h:5432/db",  # Unsupported scheme
            "postgresql+psycopg://h/db",  # SQLAlchemy-style scheme
            "postgresql://u:p@host/db",  # User name and password
            "jdbc:postgresql:db",  # No host
            "host:5432",  # No database
            "jdbc://h/db",  # Incomplete scheme
        ),
    )
    def test_postgresql_full_url_invalid(self, url: str) -> None:
        """Malformed PostgreSQL URLs are rejected, not passed to Java."""
        opt = Options(DRIVER.postgresql, url=url)
        with pytest.raises(ValueError, match="Cannot construct a JDBC URL"):
            opt.full_url

    @pytest.mark.parametrize(
        "kwargs",
        (
            dict(),  # No URL
            dict(url="host:5432/db", path="foo/bar"),  # Path
            dict(path="foo/bar"),  # Path and no URL
        ),
    )
    def test_postgresql_invalid(self, kwargs: dict[str, Any]) -> None:
        """Only a URL is valid; a path is not."""
        with pytest.raises(ValueError, match="driver='postgresql', url="):
            Options(DRIVER.postgresql, **kwargs)

    def test_postgresql_properties(self) -> None:
        """The JDBC driver class, URL, and credentials are passed to Java."""
        from ixmp.backend.jdbc.jvm import start_jvm

        start_jvm()

        opt = Options(
            DRIVER.postgresql, url="host:5432/db", user="USER", password="PASSWORD"
        )
        props = opt.properties
        assert "org.postgresql.Driver" == str(props.getProperty("jdbc.driver"))
        assert "jdbc:postgresql://host:5432/db" == str(props.getProperty("jdbc.url"))
        assert "USER" == str(props.getProperty("jdbc.user"))
        assert "PASSWORD" == str(props.getProperty("jdbc.pwd"))

    def test_from_file_postgresql(self, tmp_path: Path) -> None:
        """A properties file with a PostgreSQL driver is read correctly."""
        path = tmp_path.joinpath("postgresql.properties")
        path.write_text(
            "jdbc.driver = org.postgresql.Driver\n"
            "jdbc.url = jdbc:postgresql://host.example.com:5432/dbname\n"
            "jdbc.user = USER\n"
            "jdbc.pwd = PASSWORD\n"
            "jdbc.other = VALUE\n"
        )

        opt = Options.from_file(path, jvmargs="")

        assert DRIVER.postgresql is opt.driver
        assert "jdbc:postgresql://host.example.com:5432/dbname" == opt.full_url
        assert "USER" == opt.user
        assert "PASSWORD" == opt.password
        assert dict(other="VALUE") == opt.extra_properties

    def test_from_file_oracle(self, tmp_path: Path) -> None:
        """Drivers other than HyperSQL and PostgreSQL are still mapped to Oracle."""
        path = tmp_path.joinpath("oracle.properties")
        path.write_text(
            "jdbc.driver = oracle.jdbc.driver.OracleDriver\njdbc.url = HOST:1234:SID\n"
        )

        assert DRIVER.oracle is Options.from_file(path, jvmargs="").driver
