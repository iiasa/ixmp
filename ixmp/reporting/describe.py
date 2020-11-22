from collections.abc import Hashable
from functools import partial
from itertools import chain

import dask.core
import xarray as xr

from .key import Key


def describe_recursive(graph, comp, depth=0, seen=None):
    """Recursive helper for :meth:`ixmp.reporting.Reporter.describe`.

    Parameters
    ----------
    graph :
        A dask graph.
    comp :
        A dask computation.
    depth : int
        Recursion depth. Used for indentation.
    seen : set
        Keys that have already been described. Used to avoid
        double-printing.
    """
    comp = comp if isinstance(comp, tuple) else (comp,)
    seen = set() if seen is None else seen

    indent = (" " * 2 * (depth - 1)) + ("- " if depth > 0 else "")

    # Strings for arguments
    result = []

    for arg in comp:
        # Don't fully reprint keys and their ancestors that have been seen
        try:
            if arg in seen:
                if depth > 0:
                    # Don't print top-level items that have been seen
                    result.append(f"{indent}'{arg}' (above)")
                continue
        except TypeError:
            pass

        # Convert various types of arguments to string
        if isinstance(arg, xr.DataArray):
            # DataArray → just the first line of the string representation
            item = str(arg).split("\n")[0]
        elif isinstance(arg, partial):
            # functools.partial → less verbose format
            fn_name = arg.func.__name__
            fn_args = ", ".join(
                chain(
                    map(repr, arg.args),
                    map("{0[0]}={0[1]}".format, arg.keywords.items()),
                )
            )
            item = f"{fn_name}({fn_args}, ...)"
        elif isinstance(arg, (str, Key)) and arg in graph:
            # key that exists in the graph → recurse
            item = "'{}':\n{}".format(
                arg, describe_recursive(graph, graph[arg], depth + 1, seen)
            )
            seen.add(arg)
        elif (
            isinstance(arg, list)
            and len(arg)
            and isinstance(arg[0], Hashable)
            and arg[0] in graph
        ):
            # list → collection of items
            item = "list of:\n{}".format(
                describe_recursive(graph, tuple(arg), depth + 1, seen)
            )
            seen.update(arg)
        elif isinstance(arg, dask.core.literal):
            # Item protected with dask.core.quote()
            item = str(arg.data)
        else:
            item = str(arg)

        result.append(indent + item)

    # Combine items
    return ("\n" if depth > 0 else "\n\n").join(result)
