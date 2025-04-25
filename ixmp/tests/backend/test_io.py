from pathlib import Path

import ixmp
from ixmp.testing import add_random_model_data, models


def test_read_excel_big(test_mp: ixmp.Platform, tmp_path: Path) -> None:
    """Excel files with model items split across sheets can be read.

    https://github.com/iiasa/ixmp/pull/345.
    """
    tmp_path /= "output.xlsx"

    # Write a 25-element parameter with max_row=10 → split across 3 sheets
    scen = ixmp.Scenario(test_mp, **models["dantzig"], version="new")
    add_random_model_data(scen, 25)
    scen.to_excel(tmp_path, items=ixmp.ItemType.MODEL, max_row=10)

    # Initialize target scenario for reading
    scen_empty = ixmp.Scenario(test_mp, "foo", "bar", version="new")
    scen_empty.init_set("random_set")
    scen_empty.init_par(
        "random_par", scen.idx_sets("random_par"), scen.idx_names("random_par")
    )

    # File can be read
    scen_empty.read_excel(tmp_path)

    assert len(scen_empty.par("random_par")) == 25
