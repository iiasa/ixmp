"""Utilities for interaction with Java and ``ixmp_source`` via :mod:`jpype`."""

import logging
import os
import platform
import re
from collections.abc import Callable, Generator, Iterable, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast, overload

log = logging.getLogger(__name__)


class JClassProxy:
    """Proxy to Java packages and classes.

    This class works around the JPype limitation that classes cannot be imported and
    referenced until the JVM has been started. After a call to :meth:`setup`, Java
    classes like "at.iiasa.ac.iiasa.ixmp.Platform" can be accessed as
    :py:`java.ixmp.Platform`, or others from the Java standard library as for instance
    :py:`java.lang.Double` or :py:`java.util.HashMap`.
    """

    ixmp: Any  # NB jpype.JPackage, but this is not typed upstream
    lang: Any
    math: Any
    util: Any

    def setup(self) -> None:
        import jpype

        setattr(self, "lang", jpype.JPackage("java.lang"))
        setattr(self, "math", jpype.JPackage("java.math"))
        setattr(self, "util", jpype.JPackage("java.util"))
        setattr(self, "ixmp", jpype.JPackage("at.ac.iiasa.ixmp"))


#: Proxy to Java classes.
java = JClassProxy()


@contextmanager
def handle_jexception() -> Generator[None, Any, None]:
    """Context manager form of :func:`_raise_jexception`."""
    try:
        yield
    except java.lang.Exception as e:
        raise_jexception(e)


def raise_jexception(exc: Any, msg: str = "unhandled Java exception: ") -> None:
    """Convert Java/JPype exceptions to ordinary Python RuntimeError."""
    from ixmp.backend.jdbc import _EXCEPTION_VERBOSE

    # Try to re-raise as a ValueError for bad model or scenario name
    arg = exc.args[0] if isinstance(exc.args[0], str) else ""
    if match := re.search(r"getting '([^']*)' in table '([^']*)'", arg):
        param = match.group(2).lower()
        if param in {"model", "scenario"}:
            raise ValueError(f"{param}={repr(match.group(1))}") from None

    # Other exceptions
    if _EXCEPTION_VERBOSE:
        msg += "\n\n" + exc.stacktrace()
    else:
        msg += exc.message()

    raise RuntimeError(msg) from None


def start_jvm(jvmargs: str | list[str] | None = None) -> None:
    """Start the Java Virtual Machine via JPype_.

    Parameters
    ----------
    jvmargs : str or list of str, optional
        Additional arguments for launching the JVM, passed to :func:`jpype.startJVM`.

        For instance, to set the maximum heap space to 4 GiB, give
        ``jvmargs=['-Xmx4G']``. See the `JVM documentation`_ for a list of options.

        .. _`JVM documentation`: https://docs.oracle.com/javase/7/docs
           /technotes/tools/windows/java.html)
    """
    import jpype

    from ixmp.model.gams import gams_info

    if jvmargs is None:
        jvmargs = []
    if jpype.isJVMStarted():
        return

    # Base directory for the classpath and library path
    base = Path(__file__).parent

    # Arguments
    args = jvmargs if isinstance(jvmargs, list) else [jvmargs]

    # Append path to directories containing arch-specific libraries
    uname = platform.uname()
    paths = [
        gams_info().java_api_dir,  # GAMS system directory
        base.joinpath(uname.machine),  # Subdirectory of ixmp/backend/jdbc
    ]
    sep = ";" if uname.system == "Windows" else ":"
    args.append(f"-Djava.library.path={sep.join(map(str, paths))}")

    # Keyword arguments
    kwargs = dict(
        # Use ixmp.jar and related Java JAR files
        classpath=str(base.joinpath("*")),
        # For JPype 0.7 (raises a warning) and 0.8 (default is False). 'True' causes
        # Java string objects to be converted automatically to Python str(), as expected
        # by ixmp Python code.
        convertStrings=True,
    )

    log.debug(f"JAVA_HOME: {os.environ.get('JAVA_HOME', '(not set)')}")
    log.debug(f"jpype.getDefaultJVMPath: {jpype.getDefaultJVMPath()}")
    log.debug(f"args to startJVM: {args} {kwargs}")

    try:
        jpype.startJVM(*args, **kwargs)
    except FileNotFoundError as e:  # pragma: no cover
        # Not covered by tests. jpype.getDefaultJVMPath() tries an extensive set of
        # methods to find the JVM; it would require excessive effort to defeat these.
        raise FileNotFoundError(
            "This error may occur because you have not installed or configured a Java"
            "Runtime Environment. See the install documentation."
        ) from e

    # Set up proxy for referencing Java classes
    java.setup()


def to_pylist(jlist: Any) -> list[Any]:
    """Convert Java list types to :class:`list`."""
    try:
        return list(jlist[:])
    except Exception:
        # java.LinkedList
        return list(jlist.toArray()[:])


def to_jlist(
    arg: str | Iterable[float | int | str],
    convert: Callable[..., Any] | None = None,
) -> Any:
    """Convert :class:`list` *arg* to java.LinkedList.

    Parameters
    ----------
    arg : Collection or Iterable or str
    convert : callable, optional
        If supplied, every element of `arg` is passed through `convert` before being
        added.

    Returns
    -------
    java.util.LinkedList
    """
    # Previously JPype1 (prior to 1.0) could take single argument in addAll method of
    # Java collection. As string implements Sequence contract in Python we need to
    # convert it explicitly to list here.
    if isinstance(arg, str):
        arg = [arg]

    if convert is not None:
        return java.util.LinkedList(list(map(convert, arg)))
    elif isinstance(arg, Sequence):
        # Sized collection can be used directly
        return java.util.LinkedList(arg)
    elif isinstance(arg, Iterable):
        # Transfer items from an iterable, generator, etc. to the LinkedList
        return java.util.LinkedList(list(arg))
    else:
        raise ValueError(arg)


@overload
def unwrap(v: list[bool | float | str]) -> list[bool | float | str]: ...


@overload
def unwrap(v: bool | float | str) -> bool | float | str: ...


def unwrap(v: Any) -> bool | float | str | list[bool | float | str]:
    """Unwrap meta numeric value or list of values (BigDecimal -> Double)."""
    if isinstance(v, java.math.BigDecimal):
        _v: float = v.doubleValue()
        return _v
    elif isinstance(v, java.util.ArrayList):
        return [unwrap(elt) for elt in v]
    else:
        # NOTE In truth, this value might only be a compatible Java type
        else_v: bool | str = v
        return else_v


def wrap(value: Any) -> bool | float | int | str | list[bool | float | str]:
    if isinstance(value, (str, bool)):
        return value
    elif isinstance(value, (int, float)):
        # NOTE In truth, BigDecimal seems to return a Java type compatible with both
        # float and int
        _value: float | int = java.math.BigDecimal(value)
        return _value
    elif isinstance(value, (Sequence, Iterable)):
        jlist = java.util.ArrayList()
        jlist.addAll([wrap(elt) for elt in value])
        return cast(list[bool | float | str], jlist)
    else:
        raise ValueError(f"Cannot use value {value} as metadata")
