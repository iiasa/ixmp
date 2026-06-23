import os
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import Enum, auto
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

#: Default URL-end property/value pairs for HyperSQL databases, used by
#: :attr:`.full_url`.
#:
#: See `Chapter 14. Properties <https://www.hsqldb.org/doc/guide/dbproperties-chapt.html#dpc_db_operations>`_
#: in the HyperSQL documentation for description of this setting. This avoids
#: performance issues instantiating :class:`.Platform` instances connected to databases
#: with large amounts of data. See :issue:`643` and :issue:`433` for details.
HSQLDB_DEFAULT_PROPS = ["hsqldb.default_table_type=cached"]


class DRIVER(Enum):
    """JDBC drivers supported with :class:`JDBCBackend`.

    See :attr:`.Options.driver`.
    """

    #: HyperSQL.
    hsqldb = auto()
    #: Oracle.
    oracle = auto()

    @classmethod
    def from_str(cls, value: "str | DRIVER") -> "DRIVER":
        """Maybe convert :class:`str` to an enumeration member."""
        try:
            return cls[value] if isinstance(value, str) else value
        except KeyError:
            raise ValueError(f"unrecognized/unsupported JDBC driver {value!r}")


@dataclass
class Options:
    """Options and configuration for :class:`JDBCBackend`.

    .. autosummary::

       driver
       path
       url
       user
       password
       extra_properties
       jvmargs
    """

    #: JDBC driver to use. See :class:`DRIVER`.
    driver: DRIVER
    #: Path to a local HyperSQL database. Invalid with :any:`DRIVER.oracle`.
    path: os.PathLike[str] | None = None
    #: Partial or compelte JDBC URL, for example
    #: "database-server.example.com:PORT:SCHEMA". See :ref:`configuration`.
    url: str = ""
    #: Database username.
    user: str = ""
    #: Database user password.
    password: str = ""
    #: Arbitrary additional JDBC properties.
    extra_properties: dict[str, str] = field(default_factory=dict)
    #: Java Virtual Machine arguments. See :func:`.start_jvm`.
    jvmargs: str | list[str] = ""

    def __post_init__(self) -> None:
        # Maybe convert a str to a DRIVER member
        self.driver = DRIVER.from_str(self.driver)

        # Check consistency of fields
        if self.driver is DRIVER.oracle and (self.path or not self.url):
            raise ValueError("use JDBCBackend(driver='oracle', url=…)")
        elif self.driver is DRIVER.hsqldb and not self.path and not self.url:
            raise ValueError(
                "use JDBCBackend(driver='hsqldb', path=…) or "
                "JDBCBackend(driver='hsqldb', url=…)"
            )

        # Remaining arguments are for the JVM
        if isinstance(self.jvmargs, list):
            self.jvmargs, *extra = self.jvmargs or [""]
            if extra:
                raise ValueError(f"Extra arguments for JDBCBackend: {extra!r}")

    @property
    def full_url(self) -> str:
        """The full JDBC URL for the connection.

        With :any:`DRIVER.hsqldb` and :attr:`path` set (or the "file:" protocol in
        :attr:`url`), the :data:`HSQLDB_DEFAULT_PROPS` are appended to the constructed
        URL, *unless* the user explicitly has specified the same properties.
        """
        result = ["jdbc", self.driver.name]
        match self.driver:
            case DRIVER.oracle:
                result.extend(["thin", f"@{self.url}"])
            case DRIVER.hsqldb:
                if self.path:
                    proto = "file"
                    # Convert Windows paths to use forward slashes
                    db = str(PurePosixPath(Path(self.path).resolve())).replace("\\", "")
                    props = HSQLDB_DEFAULT_PROPS.copy()
                elif match := re.fullmatch(
                    "(jdbc:hsqldb:)?(?P<proto>file|mem):(?P<db>[^;]+)(;(?P<props>.*))?",
                    self.url,
                ):
                    proto, db, all_props = match.group("proto", "db", "props")
                    props = all_props.split(";") if all_props else []
                else:
                    raise ValueError(f"Cannot construct a JDBC URL for {self}")

                # Use cached tables by default for non-memory databases
                if proto == "file" and not any(
                    HSQLDB_DEFAULT_PROPS[0].partition("=")[0] in p for p in props
                ):
                    props.append(HSQLDB_DEFAULT_PROPS[0])

                result.append(proto)
                result.append(db + ((";".join([""] + props)) if props else ""))

        return ":".join(result)

    @property
    def properties(self) -> Any:
        """Return a :py:`java.util.Properties` instance for an ixmp (Java) Platform."""
        from .jvm import java

        result = java.util.Properties()

        for key, value in (
            {
                "jdbc.driver": {
                    DRIVER.hsqldb: "org.hsqldb.jdbcDriver",
                    DRIVER.oracle: "oracle.jdbc.driver.OracleDriver",
                }[self.driver],
                "jdbc.url": self.full_url,
                "jdbc.user": self.user or "ixmp",
                "jdbc.pwd": self.password or "ixmp",
                # commented: This has no effect, apparently because ixmp_source Platform
                # class fails to pass the property on to HyperSQL
                # "hsqldb.default_table_type": "cached",
            }
            | self.extra_properties
        ).items():
            result.setProperty(key, value)

        return result

    @classmethod
    def handle_config(cls, args: Sequence[Any], **kw: Any) -> dict[str, str]:
        """Handle CLI arguments to :program:`ixmp platform add [name] ...`.

        Parameters
        ----------
        args
            Positional arguments. These override any `kw`, and may be in the form of:

            1. :py:`("oracle", url, user, password, [jvmargs])`
            2. :py:`("hsqldb", path, [jvmargs])` for a file-backed HyperSQL database.
            3. :py:`("hsqldb",)`. with :attr:`url` supplied via `kwargs`, for instance
               "jdbc:hsqldb:mem://foo" for an in-memory database.

        Returns
        -------
        dict
            Representation of `args` and `kw` suitable for :file:`config.json`.
        """
        # Make a list copy of `args` to avoid mutating
        args = list(args)

        # Shorthands for exception formatting
        exp, got = " expected for JDBCBackend", f"; got {args}, {kw!r}"

        def _raise(text: str) -> NoReturn:
            raise ValueError(f"{text}{exp}{got}")

        # First argument: driver
        try:
            driver = DRIVER.from_str(args.pop(0))
        except IndexError:
            _raise("≥1 positional argument (driver)")
        else:
            exp += f"(driver={driver.name!r})"

        # Remaining arguments
        match driver:
            case DRIVER.oracle:
                if len(args) < 3:
                    _raise("3–4 arguments (URL, user, password, [jvmargs])")

                kw["url"], kw["user"], kw["password"], *kw["jvmargs"] = args
            case DRIVER.hsqldb:
                try:
                    kw["path"] = Path(args.pop(0)).resolve()
                except IndexError:
                    if "url" not in kw:
                        _raise("either positional path or url= keyword argument")
                kw["jvmargs"] = args

        # Convert from a Config instance back to a dict
        result = dict()
        for key, value in asdict(cls(driver, **kw)).items():
            if key == "driver":
                result["driver"] = value.name  # String name of enumeration value
            elif value:
                result[key] = value  # Non-empty item

        return result

    @classmethod
    def from_file(cls, path: os.PathLike[str], jvmargs: str | list[str]) -> "Options":
        """Read database connection properties from a file at `path`.

        Lines in `path` of the form "key = value" are parsed and returned as a new
        Options instance. The following keys are mapped to fields:

        - "jdbc.driver" → :attr:`driver`.
        - "jdbc.pwd" → :attr:`password`.
        - "jdbc.url" → :attr:`url`.
        - "jdbc.user" → :attr:`user`.

        All others are stored in :attr:`extra_properties`.

        Raises
        ------
        FileNotFoundError
            if `path` does not exist or is not a readable file.
        """
        path = Path(path)
        if not path.exists() and path.is_file():
            raise FileNotFoundError(path)

        args: dict[str, Any] = dict(path=None, extra_properties={})
        expr = re.compile(r"^(?:jdbc\.)?([\w\.]+)\s*=\s*(.+)\s*")
        for match in filter(None, map(expr.fullmatch, path.read_text().splitlines())):
            name, value = match.group(1), match.group(2)
            match name:
                case "driver":
                    args[name] = DRIVER.hsqldb if "hsqldb" in value else DRIVER.oracle
                case "pwd":
                    args["password"] = value  # Change name
                case "url" | "user":
                    args[name] = value  # Store
                case _:
                    args["extra_properties"][name] = value  # Store

        if "url" not in args:
            raise ValueError(f"File {path} contains no database URL")

        return cls(**args, jvmargs=jvmargs)
