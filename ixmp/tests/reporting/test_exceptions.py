import re

from ixmp.testing import get_cell_output, run_notebook


EXPECTED = re.compile(r"""computing 'test' using:

\(<function fail at \w+>,\)

Use Reporter.describe\(...\) to trace the computation.

Computation traceback:
  File "<ipython-input-\d*-\w+>", line 4, in fail
    'x' \+ 3.4  # Raises TypeError
TypeError: can only concatenate str \(not "float"\) to str
""")


def test_computationerror_ipython(test_data_path, tmp_path, tmp_env):
    fname = test_data_path / 'reporting-exceptions.ipynb'
    nb, _ = run_notebook(fname, tmp_path, tmp_env, allow_errors=True)

    assert EXPECTED.match(get_cell_output(nb, 0, kind='evalue'))
