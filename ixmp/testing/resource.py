"""Performance testing."""

import logging
from collections import namedtuple
from typing import Any, Optional

try:
    import resource

    has_resource_module = True
except ImportError:  # pragma: no cover
    # Windows
    has_resource_module = False

import numpy as np
import pytest

log = logging.getLogger(__name__)


#: Data structure for memory information used by :meth:`memory_usage`.
MemInfo = namedtuple("MemInfo", "profiled max_rss jvm_total jvm_free jvm_used python")


def format_meminfo(arr, cls=float):
    """Return a namedtuple for `arr`, with values as `cls`."""
    # Format strings
    cls = "{: >7.2f}".format if cls is str else cls
    return MemInfo(*map(cls, arr))


# Variables for memory_usage
_COUNT = 0
_PREV = np.zeros(6)
_RT: Optional[Any] = None


def memory_usage(message="", reset=False):
    """Profile memory usage from within a test function.

    The Python package ``memory_profiler`` and JPype_ are used to report memory
    usage. A message is logged at the ``DEBUG`` level, similar to::

       DEBUG    ixmp.testing:testing.py:527  42 <message>
       MemInfo(profiled=' 533.76', max_rss=' 534.19', jvm_total=' 213.50',
               jvm_free='  79.22', jvm_used=' 134.28', python='399.48')
       MemInfo(profiled='   0.14', max_rss='   0.00', jvm_total='   0.00',
               jvm_free=' -37.75', jvm_used='  37.75', python=' -37.61')

    Parameters
    ----------
    message : str, optional
        A string added to the log message, to identify points in profiled code.
    reset : bool, optional
        If :obj:`True`, start profiling anew.

    Returns
    -------
    collections.namedtuple
        A ``MemInfo`` tuple with the following fields, all in MiB:

        - ``profiled``: the instantaneous memory usage reported by memory_profiler.
        - ``max_rss``: the maximum resident set size (i.e. the maximum over the entire
          life of the process) reported by :meth:`resource.getrusage`.
        - ``jvm_total``: total memory allocated for the Java Virtual Machine (JVM)
          underlying JPype, used by :class:`.JDBCBackend`; the same as
          ``java.lang.Runtime.getRuntime().totalMemory()``.
        - ``jvm_free``: memory allocated to the JVM that is free.
        - ``jvm_used``: memory used by the JVM, i.e. ``jvm_total`` minus ``jvm_free``.
        - ``python``: a rough estimate of Python memory usage, i.e. ``profiled`` minus
          ``jvm_used``. This may not be accurate.
    """
    import memory_profiler

    from ixmp.backend.jdbc import java

    global _COUNT, _PREV, _RT

    if reset:
        _COUNT = 0
        _PREV = np.zeros(6)
    else:
        _COUNT += 1

    try:
        # Get the Java runtime
        runtime = _RT or java.Runtime.getRuntime()
    except AttributeError:
        # JVM not loaded
        runtime = None

    # Collect data
    result = [
        memory_profiler.memory_usage()[0],
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,  # to MiB
    ]
    if runtime:
        _RT = runtime
        result.extend(
            [
                _RT.totalMemory() / 1024**2,
                _RT.freeMemory() / 1024**2,
            ]
        )
    else:
        result.extend([0, 0])

    # JVM total - JVM free = JVM used
    result.append(result[-2] - result[-1])

    # Python used - JVM used = only-Python used?
    result.append(result[0] - result[-1])

    # Convert to a numpy array
    result = np.array(result)

    # Difference versus previous call
    delta = result - _PREV

    # Store current *result* to compute *delta* in subsequent calls
    _PREV = result

    # Log the results
    msg = "\n".join(
        [
            f"{_COUNT:3d} {message}",
            repr(format_meminfo(result, str)),
            repr(format_meminfo(delta, str)),
        ]
    )
    log.debug(msg)

    # Return the current result
    return format_meminfo(result)


@pytest.fixture(scope="function")
def resource_limit(request):
    """A fixture that limits Python :mod:`resources <resource>`.

    See the documentation (``pytest --help``) for the ``--resource-limit`` command-line
    option that selects (1) the specific resource and (2) the level of the limit.

    The original limit, if any, is reset after the test function in which the fixture is
    used.
    """
    if not has_resource_module:
        pytest.skip("Python module 'resource' not available (non-Unix OS)")

    name, value = request.config.getoption("--ixmp-resource-limit").split(":")
    res = getattr(resource, f"RLIMIT_{name.upper()}")
    value = int(value)

    if res in (resource.RLIMIT_AS, resource.RLIMIT_DATA, resource.RLIMIT_RSS):
        value = value * 1024**2  # MiB → bytes

    if value > 0:
        # Store existing limit
        before = resource.getrlimit(res)

        log.debug(f"Change {res} from {before} to ({value}, {before[1]})")
        resource.setrlimit(res, (value, before[1]))

    try:
        yield
    finally:
        if value > 0:
            log.debug(f"Restore {res} to {before}")
            resource.setrlimit(res, before)
