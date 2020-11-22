import logging
from itertools import chain
from traceback import TracebackException, format_exception_only, format_list

log = logging.getLogger(__name__)


class ComputationError(Exception):
    """Wrapper to print intelligible exception information for Reporter.get().

    In order to aid in debugging, this helper:
    - Omits the parts of the stack trace that are internal to dask, and
    - Gives the key in the Reporter.graph and the computation task that
      caused the exception.
    """

    def __init__(self, exc):
        self._exc = exc

    def __str__(self):
        """String representation.

        Most exception handling (Python, IPython, Jupyter) will print the
        traceback that led to *self* (i.e. the call to :meth:`.Reporter.get`),
        followed by the string returned by this method.
        """
        try:
            return self._format()
        except Exception as format_exc:
            # Something went wrong during _format()
            log.error(
                f"Exception raised while formatting {self._exc}:\n" + repr(format_exc)
            )
            # Fall back to printing the underlying exception
            return str(self._exc)

    def _format(self):
        key, task, frames = process_dask_tb(self._exc)

        # Assemble the exception printout
        return "".join(
            chain(
                # Reporter information for debugging
                [
                    f"computing {key} using:\n\n" if key else "",
                    f"{task}\n\n" if task else "",
                    "Use Reporter.describe(...) to trace the computation.\n\n",
                    "Computation traceback:\n",
                ],
                # Traceback; omitting a few dask internal calls below execute_task
                format_list(frames),
                # Type and message of the original exception
                format_exception_only(self._exc.__class__, self._exc),
            )
        )


def process_dask_tb(exc):
    """Process *exc* arising from :meth:`.Reporter.get`.

    Returns a tuple with 3 elements:

    - The key of the reporting computation.
    - The info key of the reporting computation.
    - A list of traceback.FrameSummary objects, without locals, for *only*
      frames that are not internal to dask.
    """
    key = task = None  # Info about the computation that triggered *exc*
    frames = []  # Frames for an abbreviated stacktrace

    try:
        # Get a traceback with captured locals
        tbe = TracebackException.from_exception(exc, capture_locals=True)
    except Exception:
        # Some exception occurred when capturing locals; proceed without
        tbe = TracebackException.from_exception(exc)

    # Iterate over frames from the base of the stack
    # Initial frames are internal to dask
    dask_internal = True
    for frame in tbe.stack:
        if frame.name == "execute_task":
            # Current frame is the dask internal call to execute a task
            try:
                # Retrieve information about the key/task that triggered the
                # exception. These are not the raw values of variables, but
                # their string repr().
                key = frame.locals["key"]
                task = frame.locals["task"]
            except (TypeError, KeyError):  # pragma: no cover
                # No locals, or 'key' or 'task' not present
                pass

            # Subsequent frames are related to the exception
            dask_internal = False

        if not dask_internal:
            # Don't display the locals when printing the traceback
            frame.locals = None

            # Store the frame for printing the traceback
            frames.append(frame)

    # Omit a few dask internal calls below execute_task
    return key, task, frames[3:]
