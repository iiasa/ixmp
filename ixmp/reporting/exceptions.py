
class ComputationError(Exception):
    """Wrapper to print intelligible exception information for Reporter.get().

    In order to aid in debugging, this helper:
    - Omits the parts of the stack trace that are internal to dask, and
    - Gives the key in the Reporter.graph and the computation task that
      caused the exception.
    """
    def __str__(self):
        from traceback import (
            TracebackException,
            format_exception_only,
            format_list,
        )

        # Move the cause to a non-private attribute
        self.cause = self.__cause__

        # Suppress automatic printing of the cause
        self.__cause__ = None

        info = None  # Information about the call that triggered the exception
        frames = []  # Frames for an abbreviated stacktrace
        dask_internal = True  # Flag if the frame is internal to dask

        try:
            # Get a traceback with captured locals
            tb = TracebackException.from_exception(self.cause,
                                                   capture_locals=True)
        except Exception:
            tb = TracebackException.from_exception(self.cause)

        # Iterate over frames from the base of the stack
        for frame in tb.stack:
            if frame.name == 'execute_task':
                # Current frame is the dask internal call to execute a task

                # Retrieve information about the key/task that triggered the
                # exception. These are not the raw values of variables, but
                # their string repr().
                try:
                    i = frame.locals
                    info = f"computing {i['key']!r} using\n\n{i['task']}\n\n"
                except (TypeError, KeyError):
                    info = ''

                # Remaining frames are related to the exception
                dask_internal = False

            if not dask_internal:
                # Don't display the locals when printing the traceback
                frame.locals = None

                # Store the frame for printing the traceback
                frames.append(frame)

        # Assemble the exception printout

        # Reporter information for debugging
        lines = [
            info,
            'Use Reporter.describe(...) to trace the computation.\n\n',
            'Computation traceback:\n',
        ]
        # Traceback; omitting a few dask internal calls below execute_task
        lines.extend(format_list(frames[3:]))
        # Type and message of the original exception
        lines.extend(format_exception_only(self.cause.__class__, self.cause))

        return ''.join(lines)
