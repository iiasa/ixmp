from genno import Quantity
from genno.testing import assert_qty_equal
import pandas as pd
import pytest

from ixmp.reporting.computations import map_as_qty

pytestmark = pytest.mark.usefixtures("parametrize_quantity_class")


def test_map_as_qty():
    b = ["b1", "b2", "b3", "b4"]
    input = pd.DataFrame(
        [["f1", "b1"], ["f1", "b2"], ["f2", "b3"]], columns=["foo", "bar"]
    )

    result = map_as_qty(input, b)

    exp = Quantity(
        pd.DataFrame(
            [
                ["f1", "b1", 1],
                ["f1", "b2", 1],
                ["f2", "b3", 1],
                ["all", "b1", 1],
                ["all", "b2", 1],
                ["all", "b3", 1],
                ["all", "b4", 1],
            ],
            columns=["foo", "bar", "value"],
        ).set_index(["foo", "bar"])
    )

    assert_qty_equal(exp, result)
