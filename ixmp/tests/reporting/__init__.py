import numpy as np
import pint
import xarray as xr

from ixmp.reporting import Quantity

REGISTRY = pint.get_application_registry()


def add_test_data(scen):
    # New sets
    t_foo = ["foo{}".format(i) for i in (1, 2, 3)]
    t_bar = ["bar{}".format(i) for i in (4, 5, 6)]
    t = t_foo + t_bar
    y = list(map(str, range(2000, 2051, 10)))

    # Add to scenario
    scen.init_set("t")
    scen.add_set("t", t)
    scen.init_set("y")
    scen.add_set("y", y)

    # Data
    ureg = pint.get_application_registry()
    x = xr.DataArray(
        np.random.rand(len(t), len(y)),
        coords=[t, y],
        dims=["t", "y"],
        attrs={"_unit": ureg.Unit("kg")},
    )
    x = Quantity(x)

    # As a pd.DataFrame with units
    x_df = x.to_series().rename("value").reset_index()
    x_df["unit"] = "kg"

    scen.init_par("x", ["t", "y"])
    scen.add_par("x", x_df)

    return t, t_foo, t_bar, x
