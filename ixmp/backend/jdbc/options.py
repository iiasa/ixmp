import os
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import Enum, auto
from pathlib import Path, PurePosixPath
from typing import Any

#: Default URL-end property/value pairs for HyperSQL databases, used by
#: :attr:`.full_url`.
#:
#: See `Chapter 14. Properties <https://www.hsqldb.org/doc/guide/dbproperties-chapt.html#dpc_db_operations>`_
#: in the HyperSQL documentation for description of this setting. This avoids
#: performance issues instantiating :class:`.Platform` instances connected to databases
#: with large amounts of data. See :issue:`643` and :issue:`433` for details.
HSQLDB_DEFAULT_PROPS = ["hsqldb.default_table_type=cached"]

#: Regular expression for JDBC URLs.
URL_PATTERN = re.compile(
    r"""
   (
     (jdbc:)?
     (?P<scheme>hsqldb|oracle|postgresql(\+psycopg)?):
   )?
   ((?P<proto>file|mem|thin):)?
   (
     (//)?
     (?P<netloc>
       (
         (?P<username>[^:]+)?
         (
          :
          (?P<password>[^@]+)
         )?
         @
       )?
       (?P<hostname>([^/:;]+|\[.*\]))
       (:(?P<port>\d+))?
     )
     /?
   )?
   (?P<db_name>[^;/?]+)?  # Database name or path
   (\?(?P<query>.*))?    # Query parameters, e.g. for PostgreSQL
   (;(?P<props>.*))?     # Properties, e.g. for HyperSQL
   """,
    re.VERBOSE,
)


class DRIVER(Enum):
    """JDBC drivers supported with :class:`JDBCBackend`.

    See :attr:`.Options.driver`.
    """

    #: HyperSQL.
    hsqldb = auto()
    #: Oracle.
    oracle = auto()
    #: PostgreSQL.
    postgresql = auto()

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

    :class:`ValueError` is raised on invalid arguments.

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

    #: Path to a local HyperSQL database. Invalid with :any:`DRIVER.oracle` and
    #: :any:`DRIVER.postgresql`.
    path: os.PathLike[str] | None = None

    #: Partial or complete JDBC URL. Valid forms differ according to :attr:`driver` as
    #: follows. Square brackets (``[...]``) indicate optional components.
    #:
    #: :any:`DRIVER.hsqldb`:
    #:    - ``"[jdbc:hsqldb:]file:/path/to/file"``
    #:    - ``"[jdbc:hsqldb:]mem:{name}"``, where ``{name}`` is any string.
    #:    - ``"[jdbc:hsqldb:]file:foo; property=property_value"``
    #:
    #: :any:`DRIVER.oracle`:
    #:    ``"[jdbc:oracle:thin:@]{hostname}:{port}/{schema}"``.
    #:    For example, if `hostname` is "database.example.com", `port` is 1521, and
    #:    `schema` is "MY_DATABASE", :attr:`url` may be
    #:    "database.example.com:1521/MY_DATABASE" or
    #:    "jdbc:oracle:thin:@database.example.com:1521/MY_DATABASE".
    #:
    #: :any:`DRIVER.postgresql`:
    #:    ``"[jdbc:postgresql://][user:password@]{hostname}[:{port}]/db_name[?param=value]"``
    #:    in which `hostname` may be a host name ("database.example.com"), an IPv4
    #:    address or an IPv6 address. If a username and password are supplied, they
    #:    override other values given for :attr:`user` and :attr:`password`.
    #:
    #: See also :ref:`configuration`.
    url: str = ""

    #: Database username.
    user: str = ""

    #: Database user password.
    password: str = ""

    #: Arbitrary additional JDBC properties.
    extra_properties: dict[str, str] = field(default_factory=dict)

    #: Java Virtual Machine arguments. See :func:`.start_jvm`.
    jvmargs: str | list[str] = ""

    # FIXME Reduce complexity from 15 → ≤13
    def __post_init__(self) -> None:  # noqa: C901
        # Maybe convert a str to a DRIVER member
        self.driver = DRIVER.from_str(self.driver)

        # Parse the URL (if any) to its parts
        parts = {}
        if match := URL_PATTERN.fullmatch(self.url or ""):
            parts = match.groupdict()

        # Replace unset user and password fields with values parsed from URL
        self.user = self.user or parts.get("username", "")
        self.password = self.password or parts.get("password", "")

        # Check consistency of fields
        if self.driver in (DRIVER.oracle, DRIVER.postgresql):
            if self.path or not self.url:
                raise ValueError(f"use JDBCBackend(driver='{self.driver.name}', url=…)")
            elif parts == {}:
                raise ValueError(f"Cannot construct a JDBC URL from {self.url!r}")
            elif not self.user or not self.password:
                raise ValueError(
                    "Cannot construct a JDBC URL without username and/or password"
                )
        elif self.driver is DRIVER.hsqldb and not self.path and not self.url:
            raise ValueError(
                "use JDBCBackend(driver='hsqldb', path=…) or "
                "JDBCBackend(driver='hsqldb', url=…)"
            )

        # Prepare the full URL
        match self.driver:
            case DRIVER.hsqldb:
                netloc = "hsqldb:"
                parts.pop("scheme")

                if parts.get("db_name"):
                    raise ValueError(self.url)

                if self.path:
                    proto = "file"
                    # Convert Windows paths to use forward slashes
                    netloc += "file:" + str(
                        PurePosixPath(Path(self.path).resolve())
                    ).replace("\\", "")
                    props = HSQLDB_DEFAULT_PROPS.copy()
                else:
                    # Handle URL parts
                    proto = parts["proto"]  # May be "file" or "mem"
                    netloc += f"{proto}:{parts.pop('hostname')}"
                    props = (parts["props"] or "").split(";")

                # Use cached tables by default for non-memory databases
                if proto == "file" and not any(
                    HSQLDB_DEFAULT_PROPS[0].partition("=")[0] in p for p in props
                ):
                    props.append(HSQLDB_DEFAULT_PROPS[0])

                parts.update(netloc=netloc, props=";".join(filter(None, props)))
            case DRIVER.oracle:
                # Prefix "thin" (for the 'thin' client), and "@" before the host name
                parts.update(netloc=f"oracle:thin:@{parts['netloc']}")
            case DRIVER.postgresql:
                parts.update(scheme=self.driver.name, netloc=None)

        self._full_url = format_url(parts)

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
        URL, *unless* the user explicitly has specified different values for the same
        properties.
        """
        return self._full_url

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
                    DRIVER.postgresql: "org.postgresql.Driver",
                }[self.driver],
                "jdbc.url": self._full_url,
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

            1. :py:`("oracle" or "postgresql", url, user, password, [jvmargs])`.
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

        # Return value
        result = {}

        # Shorthands for exception formatting
        exp, got = " expected for JDBCBackend", f"; got {args}, {kw!r}"

        if not args:
            raise ValueError(f"≥1 positional argument (driver){exp}{got}")

        # First positional argument: driver
        driver = DRIVER.from_str(args.pop(0))
        result["driver"] = driver.name
        exp += f"(driver={driver.name!r})"

        # Remaining arguments
        match driver:
            case DRIVER.hsqldb:
                if args:
                    kw["path"] = Path(args.pop(0)).resolve()
                elif "url" not in kw:
                    msg = f"either positional path or url= keyword argument{exp}{got}"
                    raise ValueError(msg)
            case _:
                for key in ["url", "user", "password"]:
                    if args:
                        kw[key] = args.pop(0)

        kw["jvmargs"] = args

        # Convert `kw` to a Options instance, then back to a dict
        for key, value in asdict(cls(driver, **kw)).items():
            if key != "driver" and value:
                # Non-empty item
                result[key] = value

        return result

    @classmethod
    def from_file(cls, path: os.PathLike[str], jvmargs: str | list[str]) -> "Options":
        """Read database connection properties from a file at `path`.

        Lines in `path` of the form "key = value" are parsed and returned as a new
        Options instance. The following keys are mapped to fields:

        - "jdbc.driver" → :attr:`driver`.

          If the value contains "hsqldb", it is mapped to :any:`DRIVER.hsqldb`; if it
          contains "postgresql", to :any:`DRIVER.postgresql`. Any other value, such as
          "oracle.jdbc.driver.OracleDriver", is mapped to :any:`DRIVER.oracle`.

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
                    if "hsqldb" in value:
                        args[name] = DRIVER.hsqldb
                    elif "postgresql" in value:
                        args[name] = DRIVER.postgresql
                    else:
                        # All other values
                        args[name] = DRIVER.oracle
                case "pwd":
                    args["password"] = value  # Change name
                case "url" | "user":
                    args[name] = value  # Store
                case _:
                    args["extra_properties"][name] = value  # Store

        if "url" not in args:
            raise ValueError(f"File {path} contains no database URL")

        return cls(**args, jvmargs=jvmargs)


def format_url(parts: dict[str, str]) -> str:
    """Format a JDBC URL by reassembling `parts`.

    The result always begins with "jdbc:".
    """
    result = "jdbc:"
    if scheme := parts.get("scheme"):
        result += scheme + "://"

    if netloc := parts.get("netloc"):
        # Full netloc as one string
        result += netloc
    else:
        # Hostname and port, without username or password
        result += parts["hostname"]
        if port := parts.get("port"):
            result += ":" + port

    if db_name := parts.get("db_name"):
        result += "/" + db_name

    # Either query parameters (PostgreSQL) or properties (HyperSQL)
    if query := parts.get("query"):
        result += "?" + query
    elif props := parts.get("props"):
        result += ";" + props

    return result
