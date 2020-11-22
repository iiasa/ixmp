import re

from ixmp.reporting import ComputationError
from ixmp.testing import assert_logs, get_cell_output, run_notebook


def test_computationerror(caplog):
    ce_none = ComputationError(None)

    # Message ends with ',)' on Python 3.6, only ')' on Python 3.7
    msg = (
        "Exception raised while formatting None:\nAttributeError"
        "(\"'NoneType' object has no attribute '__traceback__'\""
    )
    with assert_logs(caplog, msg):
        str(ce_none)


# The TypeError message differs:
# - Python 3.6: "must be str, not float"
# - Python 3.7: "can only concatenate str (not "float") to str"
EXPECTED = re.compile(
    r"""computing 'test' using:

\(<function fail at \w+>,\)

Use Reporter.describe\(...\) to trace the computation.

Computation traceback:
  File "<ipython-input-\d*-\w+>", line 4, in fail
    'x' \+ 3.4  # Raises TypeError
TypeError: .*str.*float.*
"""
)


def test_computationerror_ipython(test_data_path, tmp_path, tmp_env):
    # NB this requires nbformat >= 5.0, because the output kind "evalue" was
    #    different pre-5.0
    fname = test_data_path / "reporting-exceptions.ipynb"
    nb, _ = run_notebook(fname, tmp_path, tmp_env, allow_errors=True)

    observed = get_cell_output(nb, 0, kind="evalue")
    assert EXPECTED.match(observed), observed
