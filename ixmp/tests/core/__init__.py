import pandas as pd

# Test data used by test_scenario.py and test_timeseries.py.
#
# NB the columns are in a specific order; model and scenario come last in the data
#    returned by ixmp.
# TODO fix this; model and scenario should come first, matching the IAMC order.
DATA = {
    0: pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2010, 2020],
            value=[23.7, 23.8],
            model="model name",
            scenario="scenario name",
        )
    ),
    2010: pd.DataFrame.from_dict(
        {
            "region": ["World"],
            "variable": ["Testing"],
            "unit": ["???"],
            "2010": [23.7],
            "2020": [23.8],
        }
    ),
    2030: pd.DataFrame.from_dict(
        dict(
            region="World",
            variable=["Testing", "Testing", "Testing2"],
            unit="???",
            year=[2020, 2030, 2030],
            value=[24.8, 24.9, 25.1],
            model="model name",
            scenario="scenario name",
        )
    ),
    2050: pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2000, 2010, 2020, 2030, 2040, 2050],
            value=[21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
            model="model name",
            scenario="scenario name",
        )
    ),
    # NB the columns for geodata methods are inconsistent with those for time-series
    #    data
    "geo": pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="var1",
            subannual="Year",
            year=[2000, 2010, 2020],
            value=["test", "more-test", "2020-test"],
            unit="score",
            meta=0,
        )
    ),
    "timeseries": pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2010, 2020],
            value=[23.7, 23.8],
        )
    ),
}
