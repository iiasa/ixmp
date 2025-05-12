import re
from pathlib import Path
from shutil import copyfile
from typing import TYPE_CHECKING

import numpy.testing as npt
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import ixmp
from ixmp.testing import assert_logs, make_dantzig, models

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from ixmp.core.platform import Platform


# Fixtures
@pytest.fixture(scope="class")
def scen(mp):
    """The default version of the Dantzig on the mp."""
    yield ixmp.Scenario(mp, **models["dantzig"])


@pytest.fixture(scope="function")
def scen_empty(request, test_mp):
    """An empty Scenario with a temporary name on the test_mp."""
    yield ixmp.Scenario(
        test_mp,
        model=request.node.nodeid.replace("/", " "),
        scenario="test",
        version="new",
    )


@pytest.fixture(scope="function")
def test_dict():
    return {
        "test_string": "test12345",
        "test_number": 123.456,
        "test_number_negative": -123.456,
        "test_int": 12345,
        "test_bool": True,
        "test_bool_false": False,
    }


class TestScenario:
    """Tests of :class:`ixmp.Scenario`."""

    # Initialize Scenario
    # NOTE IXMP4-backed Scenarios start with version = 1
    @pytest.mark.jdbc
    def test_init(self, test_mp, scen_empty):
        # Empty scenario has version == 0
        assert scen_empty.version == 0

        # A scenario with scheme='MESSAGE' can only be created with a subclass
        class Scenario(ixmp.Scenario):
            pass

        scen2 = Scenario(
            test_mp, model="foo", scenario="bar", scheme="MESSAGE", version="new"
        )

        # JDBCBackend complains unless these items are added
        scen2.add_set("technology", "t")
        scen2.add_set("year", "2000")
        scen2.commit("")
        del scen2

        # Loading this "MESSAGE-scheme" scenario with ixmp.Scenario raises an
        # exception
        with pytest.raises(RuntimeError):
            ixmp.Scenario(test_mp, model="foo", scenario="bar")

        # …but loading with a subclass of ixmp.Scenario is fine
        Scenario(test_mp, model="foo", scenario="bar")

        with pytest.warns(
            DeprecationWarning, match=re.escape("Scenario(…, cache=…) is deprecated")
        ):
            Scenario(test_mp, model="foo", scenario="bar", cache=False)

    def test_default_version(self, mp: ixmp.Platform) -> None:
        scen = ixmp.Scenario(mp, **models["dantzig"])
        scenario_df = mp.scenario_list(
            model=models["dantzig"]["model"], scen=models["dantzig"]["scenario"]
        )
        assert len(scenario_df["version"]) == 1
        assert scen.version == scenario_df["version"].item()

    # NOTE IXMP4(Backend) doesn't raise the same error/message as expected here
    @pytest.mark.jdbc
    def test_from_url(self, mp, caplog):
        url = f"ixmp://{mp.name}/Douglas Adams/Hitchhiker"

        # Default version is loaded
        scen, mp = ixmp.Scenario.from_url(url)
        scenario_df = mp.scenario_list(
            model=models["h2g2"]["model"], scen=models["h2g2"]["scenario"]
        )
        assert len(scenario_df["version"]) == 1
        assert scen.version == scenario_df["version"].item()

        # Giving an invalid version with errors='raise' raises an exception
        expected = (
            "There exists no Scenario 'Douglas Adams|Hitchhiker' "
            "(version: 10000)  in the database!"
        )
        with pytest.raises(Exception, match=expected):
            scen, mp = ixmp.Scenario.from_url(url + "#10000", errors="raise")

        # Giving an invalid scenario with errors='warn' causes a message to be logged
        msg = (
            "ValueError: scenario='Hitchhikerfoo'\n"
            f"when loading Scenario from url: {repr(url + 'foo')}"
        )
        with assert_logs(caplog, msg):
            scen, mp = ixmp.Scenario.from_url(url + "foo")
        assert scen is None and isinstance(mp, ixmp.Platform)

    # Clone Scenario
    def test_clone(self, mp):
        scen = ixmp.Scenario(mp, **models["dantzig"], version=1)
        scen.remove_solution()
        scen.check_out()
        scen.init_set("h")
        scen.add_set("h", "test")
        scen.commit("adding an index set 'h', with element 'test'")

        scen2 = scen.clone(keep_solution=False)
        # Cloned scenario contains added set
        obs = scen2.set("h")
        npt.assert_array_equal(obs, ["test"])

    # FIXME IXMP4Backend needs to handle change_scalar correctly
    @pytest.mark.jdbc
    def test_clone_edit(self, scen):
        scen2 = scen.clone(keep_solution=False)
        scen2.check_out()
        scen2.change_scalar("f", 95.0, "USD/km")
        scen2.commit("change transport cost")

        # Original scenario that was clone is untouched
        assert scen.scalar("f") == {"unit": "USD/km", "value": 90}

        # Value is changed only in the clone
        assert scen2.scalar("f") == {"unit": "USD/km", "value": 95}

    def test_clone_scheme(self, request: "FixtureRequest", test_mp: "Platform") -> None:
        """:attr:`.Scenario.scheme` is preserved on clone."""
        # Create a string to be used as scheme name
        # NB this fails with JDBCBackend when using .nodeid directly, e.g.
        #    "ixmp tests core test_scenario.py::TestScenario::test_clone_scheme[jdbc]".
        #    This may indicate some (undocumented) restriction in values that this
        #    back end can handle.
        scheme = str(hash(request.node.nodeid))

        m_s = dict(model="test_clone_scheme", scenario="s")
        s0 = ixmp.Scenario(test_mp, **m_s, version="new", scheme=scheme)
        s0.commit("")

        # FIXME The test fails on IXMP4Backend without this statement, but passes on
        #       JDBCBackend. Adjust the behaviour of the former to match.
        s0.set_as_default()

        # Discard the reference to `s0` and load again as a new instance of Scenario
        del s0
        s0 = ixmp.Scenario(test_mp, **m_s)

        # New instance has same scheme as the original instance
        assert scheme == s0.scheme

        s1 = s0.clone()

        # Clone has same scheme as original scenario
        assert s1.scheme == s0.scheme == scheme

    # Initialize items
    # NOTE IXMP4Backend doesn't handle commits yet
    @pytest.mark.jdbc
    def test_init_set(self, scen):
        """Test ixmp.Scenario.init_set()."""

        # Add set on a locked scenario
        with pytest.raises(
            RuntimeError,
            match="This Scenario cannot be edited, do a checkout first!",
        ):
            scen.init_set("foo")

        scen = scen.clone(keep_solution=False)
        scen.check_out()
        scen.init_set("foo")

        # Initialize an already-existing set
        with pytest.raises(ValueError, match="'foo' already exists"):
            scen.init_set("foo")

    # FIXME IXMP4Backend recognized 'foo' as having no idx_sets, likely due to the
    # assumption of all names of all items being unique. We should drop that, it seems.
    @pytest.mark.jdbc
    def test_init_par(self, scen) -> None:
        scen = scen.clone(keep_solution=False)
        scen.check_out()

        # Parameter can be initialized with a tuple (not list) of idx_sets
        scen.init_par("foo", idx_sets=("i", "j"))

        # Return type of idx_sets is still list
        assert scen.idx_sets("foo") == ["i", "j"]

        # Mismatched sets and names
        with pytest.raises(ValueError, match="must have the same length"):
            scen.init_par("bar", idx_sets=("i", "j"), idx_names=("a", "b", "c"))

    def test_init_scalar(self, scen):
        scen2 = scen.clone(keep_solution=False)
        scen2.check_out()
        scen2.init_scalar("g", 90.0, "USD/km")
        scen2.commit("adding a scalar 'g'")

    # Existence checks
    # TODO IXMP4Backend doesn't handle scalars correctly yet
    @pytest.mark.jdbc
    def test_has_par(self, scen):
        assert scen.has_par("f")
        assert not scen.has_par("m")

    def test_has_set(self, scen):
        assert scen.has_set("i")
        assert not scen.has_set("k")

    def test_has_var(self, scen):
        assert scen.has_var("x")
        assert not scen.has_var("y")

    # TODO IXMP4Backend doesn't handle scalars correctly yet
    @pytest.mark.jdbc
    def test_scalar(self, scen):
        assert scen.scalar("f") == {"unit": "USD/km", "value": 90}

    # Store data
    @pytest.mark.parametrize(
        "args, kwargs",
        (
            # Scalar values/units/comment are broadcast across multiple keys
            (
                ("b", ["new-york", "chicago"]),
                dict(value=100, unit="cases", comment="c"),
            ),
            # Empty DataFrame can be added without error
            (("b", pd.DataFrame(columns=["i", "j", "value", "unit"])), dict()),
            # Exceptions
            pytest.param(
                ("b", ["new-york", "chicago"]),
                dict(value=[100, 200, 300]),
                marks=pytest.mark.xfail(
                    raises=ValueError, reason="Length mismatch between keys and values"
                ),
            ),
            pytest.param(
                ("b", pd.DataFrame(columns=["i", "j", "value", "unit"])),
                dict(value=1.0),
                marks=pytest.mark.xfail(
                    raises=ValueError,
                    reason="both key_or_data.value and value supplied",
                ),
            ),
            pytest.param(
                ("b", pd.DataFrame(columns=["i", "j", "unit"])),
                dict(),
                marks=pytest.mark.xfail(
                    raises=ValueError, reason="no parameter values"
                ),
            ),
        ),
    )
    def test_add_par(self, scen, args, kwargs):
        scen = scen.clone(keep_solution=False)
        scen.check_out()
        scen.add_par(*args, **kwargs)

    # TODO IXMP4Backend should support this, I think
    @pytest.mark.jdbc
    def test_add_par2(self, scen):
        scen = scen.clone(keep_solution=False)
        scen.check_out()
        # Create a parameter with duplicate indices
        scen.init_par("foo", idx_sets=["i", "i", "j"], idx_names=["i0", "i1", "j"])
        scen.add_par("foo", pd.DataFrame(columns=["i0", "i1", "j"]), value=1.0)

    def test_add_set(self, scen_empty) -> None:
        # NB See also test_set(), below
        scen = scen_empty

        # Initialize a 0-D set
        scen.init_set("i")
        scen.init_set("foo", idx_sets=["i"])

        # Exception raised on invalid arguments
        with pytest.raises(ValueError, match="ambiguous; both key.*"):
            scen.add_set("i", pd.DataFrame(columns=["i", "comment"]), comment="FOO")

        # Bare str for 1-D set key is wrapped automatically
        scen.add_set("i", "i0")
        scen.add_set("foo", "i0")
        assert {"i0"} == set(scen.set("foo")["i"])

    # Retrieve data
    def test_idx(self, scen):
        assert scen.idx_sets("d") == ["i", "j"]
        assert scen.idx_names("d") == ["i", "j"]

    def test_par(self, scen: "ixmp.Scenario") -> None:
        """Parameter data can be retrieved with filters."""
        df = scen.par("d", filters={"i": ["seattle"]})

        # Data frame has the expected columns
        assert ["i", "j", "value", "unit"] == list(df.columns)

        # The expected number of values are retrieved
        assert 3 == len(df)

        # Units are as expected
        # NB This test is insensitive to the contents of df.index since (as of #575) we
        #    are not certain if a RangeIndex is promised for data frames returned by
        #    Scenario.par()
        assert "km" == df["unit"].iloc[0]

        with pytest.warns(DeprecationWarning, match="ignored kwargs"):
            scen.par("d", i=["seattle"])

    # FIXME IXMP4Backend is missing an item, likely the scalar f again
    @pytest.mark.jdbc
    def test_items0(self, scen):
        # Without filters
        iterator = scen.items()

        # next() can be called → an iterator was returned
        with pytest.warns(FutureWarning, match="par_data=False will be the default"):
            next(iterator)

        # Iterator returns the expected parameter names
        exp = ["a", "b", "d", "f"]
        for i, (name, data) in enumerate(scen.items(par_data=True)):
            # Name is correct in the expected order
            assert exp[i] == name
            # Data is one of the return types of .par()
            assert isinstance(data, (pd.DataFrame, dict))

        # Total number of items was correct
        assert i == 3

        # With filters
        iterator = scen.items(filters=dict(i=["seattle"]), par_data=True)
        exp = [("a", 1), ("d", 3)]
        for i, (name, data) in enumerate(iterator):
            # Name is correct in the expected order
            assert exp[i][0] == name
            # Number of (filtered) rows is as expected
            assert exp[i][1] == len(data)

        assert i == 1

    # FIXME For test case 3, IXMP4Backend somehow also lists 'foo' (likely defined in
    # the fixture); in test case 2 it's missing f (the scalar)
    @pytest.mark.jdbc
    @pytest.mark.parametrize(
        "item_type, indexed_by, exp",
        (
            (ixmp.ItemType.EQU, None, ["cost", "demand", "supply"]),
            (ixmp.ItemType.PAR, None, ["a", "b", "d", "f"]),
            (ixmp.ItemType.SET, None, ["i", "j"]),
            (ixmp.ItemType.VAR, None, ["x", "z"]),
            # With indexed_by=
            (ixmp.ItemType.EQU, "i", ["supply"]),
            (ixmp.ItemType.PAR, "i", ["a", "d"]),
            (ixmp.ItemType.SET, "i", []),
            (ixmp.ItemType.VAR, "i", ["x"]),
        ),
    )
    def test_items1(self, scen, item_type, indexed_by, exp):
        # Function runs and yields the expected sequence of item names
        assert exp == list(scen.items(item_type, indexed_by=indexed_by, par_data=False))

    def test_items2(self, caplog, scen):
        item_type = ixmp.ItemType.SET

        list(scen.items(item_type, filters={"foo": "bar"}))

        # Warning is logged
        assert (
            "Scenario.items(…, filters=…) has no effect for item type 'set'"
            in caplog.messages
        )

    def test_var(self, scen):
        df = scen.var("x", filters={"i": ["seattle"]})

        # Labels along the 'j' dimension
        npt.assert_array_equal(df["j"], ["new-york", "chicago", "topeka"])
        # Levels
        npt.assert_array_almost_equal(df["lvl"], [50, 300, 0])
        # Marginals
        npt.assert_array_almost_equal(df["mrg"], [0, 0, 0.036])

    # TODO IXMP4Backend is not handling _cache correctly
    @pytest.mark.jdbc
    def test_load_scenario_data(self, mp):
        """load_scenario_data() caches all data."""
        scen = ixmp.Scenario(mp, **models["dantzig"])
        scen.load_scenario_data()

        cache_key = scen.platform._backend._cache_key(scen, "par", "d")

        # Item exists in cache
        assert cache_key in scen.platform._backend._cache

        # Cache has not been used
        hits_before = scen.platform._backend._cache_hit.get(cache_key, 0)
        assert hits_before == 0

        # Retrieving the expected value
        assert "km" == scen.par("d", filters={"i": ["seattle"]}).loc[0, "unit"]

        # Cache was used to return the value
        hits_after = scen.platform._backend._cache_hit[cache_key]
        assert hits_after == hits_before + 1

    def test_load_scenario_data_clear_cache(self, monkeypatch, mp):
        # this fails on commit: 4376f54
        scen = ixmp.Scenario(mp, **models["dantzig"])
        scen.load_scenario_data()
        scen.platform._backend.cache_invalidate(scen, "par", "d")

        # With cache disabled, the method fails
        monkeypatch.setattr(scen.platform._backend, "cache_enabled", False)
        with pytest.raises(ValueError, match="Cache must be enabled"):
            scen.load_scenario_data()

    # I/O
    # TODO For IXMP4Backend, this somehow triggers a GAMS-related error, while this test
    # should never touch GAMS
    @pytest.mark.jdbc
    def test_excel_io(self, scen, scen_empty, tmp_path, caplog):
        tmp_path /= "output.xlsx"

        # FIXME remove_solution, check_out, commit, solve, commit should not
        #       be needed to make this small data addition.
        scen.remove_solution()
        scen.check_out()

        # A 1-D set indexed by another set
        scen.init_set("foo", "j")
        scen.add_set("foo", [["new-york"], ["topeka"]])
        # A scalar parameter with unusual units
        scen.platform.add_unit("pounds")
        scen.init_scalar("bar", 100, "pounds")
        # A parameter with no values
        scen.init_par("baz_1", ["i", "j"])
        # A parameter with ambiguous index name
        scen.init_par("baz_2", ["i"], ["i_dim"])
        scen.add_par("baz_2", dict(value=[1.1], i_dim=["seattle"]))
        # A 2-D set with ambiguous index names
        scen.init_set("baz_3", ["i", "i"], ["i", "i_also"])
        scen.add_set("baz_3", [["seattle", "seattle"]])
        # A set with no elements
        scen.init_set("foo_2", ["j"])

        scen.commit("")
        scen.solve()

        # Solved Scenario can be written to file
        scen.to_excel(tmp_path, items=ixmp.ItemType.MODEL)

        # With init_items=False, can't be read into an empty Scenario.
        # Exception raised is the first index set, alphabetically
        with pytest.raises(ValueError, match="no set 'i'; try init_items=True"):
            scen_empty.read_excel(tmp_path)

        # File can be read with init_items=True
        scen_empty.read_excel(tmp_path, init_items=True, commit_steps=True)

        # Contents of the Scenarios are the same, except for unreadable items
        assert set(scen_empty.par_list()) | {"baz_1", "baz_2"} == set(scen.par_list())
        assert set(scen_empty.set_list()) | {"baz_3"} == set(scen.set_list())
        assert_frame_equal(scen_empty.set("foo"), scen.set("foo"))
        # NB could make a more exact comparison of the Scenarios

        # Pre-initialize skipped items 'baz_2' and 'baz_3'
        scen_empty.check_out()
        scen_empty.init_par("baz_2", ["i"], ["i_dim"])
        scen_empty.init_set("baz_3", ["i", "i"], ["i", "i_also"])

        # Data can be read into an existing Scenario without init_items or
        # commit_steps arguments
        scen_empty.read_excel(tmp_path)

        # Re-initialize an item with different index names
        scen_empty.check_out()
        scen_empty.remove_par("d")
        scen_empty.init_par("d", idx_sets=["i", "j"], idx_names=["I", "J"])

        # Reading now logs an error about conflicting dims
        with assert_logs(caplog, "Existing par 'd' has index names(s)"):
            scen_empty.read_excel(tmp_path, init_items=True)

        # A new, empty Platform (different from the one under scen -> mp ->
        # test_mp) that lacks all units
        mp = ixmp.Platform(
            backend="jdbc", driver="hsqldb", url="jdbc:hsqldb:mem:excel_io"
        )
        # A Scenario without the 'dantzig' scheme -> no contents at all
        s = ixmp.Scenario(
            mp, model="foo", scenario="bar", scheme="empty", version="new"
        )

        # Fails with add_units=False
        with pytest.raises(
            ValueError, match="The unit 'pounds' does not exist in the database!"
        ):
            s.read_excel(tmp_path, init_items=True)

        # Succeeds with add_units=True
        s.read_excel(tmp_path, add_units=True, init_items=True)

    # NOTE IXMP4-backed Scenarios should not call remove_solution() if they don't have
    # one
    @pytest.mark.jdbc
    def test_solve(self, tmp_path, scen):
        from subprocess import run

        # Copy the dantzig model file into the `tmp_path`
        model_file = tmp_path.joinpath("dantzig.gms")
        copyfile(
            Path(ixmp.__file__).parent.joinpath("model", "dantzig.gms"), model_file
        )

        scen.remove_solution()

        # Scenario solves successfully
        scen.solve(
            model_file=model_file,
            # When this is True, GAMSModel automatically cleans up the temporary
            # directory in which the model runs, including the I/O GDX files. Leave
            # them in `tmp_path` so they can be inspected
            use_temp_dir=False,
            # Include the name of a non-existent/not installed package: this should be
            # handled without triggering an error.
            record_version_packages=("ixmp", "notapackage"),
        )

        # Check both the GDX input and output files
        for part in "in", "out":
            path = str(model_file.with_name(f"dantzig_{part}.gdx"))

            # ixmp_version is present in the GDX file
            result = run(["gdxdump", path, "Symb=ixmp_version"], capture_output=True)

            # ixmp_version contains the expected contents
            assert "'ixmp'.'3-" in result.stdout.decode()
            assert "'notapackage'.'(not installed)'" in result.stdout.decode()

    # Combined tests
    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_meta(self, mp, test_dict):
        scen = ixmp.Scenario(mp, **models["dantzig"], version=1)
        for k, v in test_dict.items():
            scen.set_meta(k, v)

        # test all
        obs_dict = scen.get_meta()
        for k, exp in test_dict.items():
            obs = obs_dict[k]
            assert obs == exp

        # test name
        obs = scen.get_meta("test_string")
        exp = test_dict["test_string"]
        assert obs == exp

        scen.remove_meta(["test_int", "test_bool"])
        obs = scen.get_meta()
        assert len(obs) == 4
        assert set(obs.keys()) == {
            "test_string",
            "test_number",
            "test_number_negative",
            "test_bool_false",
        }

        # Setting with a type other than int, float, bool, str raises TypeError
        with pytest.raises(ValueError, match="Cannot use value"):
            scen.set_meta("test_string", complex(1, 1))

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_meta_bulk(self, mp, test_dict):
        scen = ixmp.Scenario(mp, **models["dantzig"], version=1)
        scen.set_meta(test_dict)

        # test all
        obs_dict = scen.get_meta()
        for k, exp in test_dict.items():
            obs = obs_dict[k]
            assert obs == exp

        # check updating metadata (replace and append)
        scen.set_meta({"test_int": 1234567, "new_attr": "new_attr"})
        assert scen.get_meta("test_int") == 1234567
        assert scen.get_meta("new_attr") == "new_attr"


def test_range(scen_empty):
    scen = scen_empty

    scen.init_set("ii")
    ii = range(1, 20, 2)

    # range instance is automatically converted to list of str in add_set
    scen.add_set("ii", ii)

    scen.init_par("new_par", idx_sets="ii")

    # range instance is a valid key argument to add_par
    scen.add_par("new_par", ii, [1.2] * len(ii))


def test_gh_210(scen_empty):
    scen = scen_empty
    i = ["i0", "i1", "i2"]

    scen.init_set("i")
    scen.add_set("i", i)
    scen.init_par("foo", idx_sets="i")

    columns = ["i", "value"]
    foo_data = pd.DataFrame(zip(i, [10, 20, 30]), columns=columns)

    # foo_data is not modified by add_par()
    scen.add_par("foo", foo_data)
    assert all(foo_data.columns == columns)


# NOTE IXMP4(Backend) raises an OptimizationDataValidationError if 'bar' is missing from
# index set 'i' instead of a ValueError
@pytest.mark.jdbc
def test_set(scen_empty) -> None:
    """Test ixmp.Scenario.add_set(), .set(), and .remove_set()."""
    scen = scen_empty

    # Add element to a non-existent set
    with pytest.raises(KeyError, match=repr("foo")):
        scen.add_set("foo", "bar")

    # Initialize a 0-D set
    scen.init_set("i")

    # Add elements to a 0-D set
    scen.add_set("i", "i1")  # Name only
    scen.add_set("i", "i2", "i2 comment")  # Name and comment
    scen.add_set("i", ["i3"])  # List of names, length 1
    scen.add_set("i", ["i4", "i5"])  # List of names, length >1
    scen.add_set("i", range(0, 3))  # Generator (range object)
    # Lists of names and comments, length 1
    scen.add_set("i", ["i6"], ["i6 comment"])
    # Lists of names and comments, length >1
    scen.add_set("i", ["i7", "i8"], ["i7 comment", "i8 comment"])

    # Incorrect usage

    # Lists of different length
    with pytest.raises(ValueError, match="Comment 'extra' without matching key"):
        scen.add_set("i", ["i9"], ["i9 comment", "extra"])
    with pytest.raises(ValueError, match="Key 'extra' without matching comment"):
        scen.add_set("i", ["i9", "extra"], ["i9 comment"])
    # Incorrect type
    with pytest.raises(
        TypeError, match="must be str or list of str; got <class 'dict'>"
    ):
        scen.add_set("i", dict(foo="bar"))

    # Add elements to a 1D set
    scen.init_set("foo", "i", "dim_i")
    scen.add_set("foo", ["i1"])  # Single key
    scen.add_set("foo", ["i2"], "i2 in foo")  # Single key and comment
    scen.add_set("foo", "i3")  # Bare name automatically wrapped
    # Lists of names and comments, length 1
    scen.add_set("foo", ["i6"], ["i6 comment"])
    # Lists of names and comments, length >1
    scen.add_set("foo", [["i7"], ["i8"]], ["i7 comment", "i8 comment"])
    # Dict
    scen.add_set("foo", dict(dim_i=["i7", "i8"]))

    # Incorrect usage
    # Improperly wrapped keys
    with pytest.raises(
        ValueError,
        match=r"2-D key \['i4', 'i5'\] invalid for " r"1-D set foo\['dim_i'\]",
    ):
        scen.add_set("foo", ["i4", "i5"])
    with pytest.raises(ValueError):
        scen.add_set("foo", range(0, 3))
    # Lists of different length
    with pytest.raises(ValueError, match="Comment 'extra' without matching key"):
        scen.add_set("i", ["i9"], ["i9 comment", "extra"])
    with pytest.raises(ValueError, match="Key 'extra' without matching comment"):
        scen.add_set("i", ["i9", "extra"], ["i9 comment"])
    # Missing element in the index set
    with pytest.raises(
        ValueError, match="The index set 'i' does not have an element 'bar'!"
    ):
        scen.add_set("foo", "bar")

    # Retrieve set elements
    i = {"i1", "i2", "i3", "i4", "i5", "0", "1", "2", "i6", "i7", "i8"}
    assert i == set(scen.set("i"))

    # Remove elements from an 0D set: bare name
    scen.remove_set("i", "i2")
    i -= {"i2"}
    assert i == set(scen.set("i"))

    # Remove elements from an 0D set: Iterable of names, length >1
    scen.remove_set("i", ["i4", "i5"])
    i -= {"i4", "i5"}
    assert i == set(scen.set("i"))

    # Remove elements from a 1D set: Dict
    scen.remove_set("foo", dict(dim_i=["i7", "i8"]))
    # Added elements from above; minus directly removed elements; minus i2
    # because it was removed from the set i that indexes dim_i of foo
    foo = {"i1", "i2", "i3", "i6", "i7", "i8"} - {"i2"} - {"i7", "i8"}
    assert foo == set(scen.set("foo")["dim_i"])

    # Remove a set completely
    assert "h" not in scen.set_list()

    scen.init_set("h")
    assert "h" in scen.set_list()

    scen.remove_set("h")
    assert "h" not in scen.set_list()


def test_filter_str(scen_empty):
    scen = scen_empty

    elements = ["foo", 42, object()]
    expected = list(map(str, elements))

    scen.init_set("s")

    # Set elements can be added which are not str
    scen.add_set("s", elements)

    # Elements are stored and returned as str
    assert expected == scen.set("s").tolist()

    # Parameter defined over 's'
    p = pd.DataFrame.from_records(
        zip(elements, [1.0, 2.0, 3.0]), columns=["s", "value"]
    )

    # Expected return dtypes of index and value columns
    dtypes = {"s": str, "value": float}
    p_exp = p.astype(dtypes)

    scen.init_par("p", ["s"])
    scen.add_par("p", p)

    # Values can be retrieved using non-string filters
    exp = p_exp.loc[1:, :].reset_index(drop=True)
    obs = scen.par("p", filters={"s": elements[1:]})
    assert_frame_equal(exp[["s", "value"]], obs[["s", "value"]])


# FIXME Calling scen.solve(change_distance, ...) triggers a bug in the GAMS code
# IXMP4Backend might need to avoid similar calls (which properties exactly?)
@pytest.mark.jdbc
def test_solve_callback(test_mp, request):
    """Test the callback argument to Scenario.solve().

    In real usage, callback() would compute some kind of convergence criterion. This
    test uses a sequence of different values for d(seattle, new-york) in Dantzig's
    transport problem. Once the correct value is set on the ixmp.Scenario, the solution
    equals an expected value, and the model has 'converged'.
    """
    # Set up the Dantzig problem
    scen = make_dantzig(test_mp, request=request)

    # Solve the scenario as configured
    solve_args = dict(model="dantzig", quiet=True)
    scen.solve(**solve_args)

    # Store the expected value of the decision variable, x
    expected = scen.var("x")

    # The reference distance between Seattle and New York is 2.5 [10^3 miles]
    d = [3.5, 2.0, 2.7, 2.5, 1.0]

    def set_d(scenario, value):
        """Set the distance between Seattle and New York to *value*."""
        scenario.remove_solution()
        scenario.check_out()
        data = {"i": "seattle", "j": "new-york", "value": value, "unit": "km"}
        # TODO should not be necessary here to call pd.DataFrame
        scenario.add_par("d", pd.DataFrame(data, index=[0]))
        scenario.commit("iterative solution")

    # Changing the entry in the array 'd' results in an optimal 'x' that is different
    # from the one stored as `expected`.
    set_d(scen, d[0])

    def change_distance(scenario):
        """Callback for model solution."""
        # Check if the model has 'converged' on the correct solution
        if (scenario.var("x") == expected).all(axis=None):
            return True

        # Convergence not reached

        # Change the distance between Seattle and New York, using the 'iteration'
        # variable stored on the Scenario object.
        set_d(scenario, d[scenario.iteration])

        # commented: see below
        # # Trigger another solution of the model
        # return False

    # Warning is raised because 'return False' is commented above, meaning user may
    # have forgotten any return statement in the callback
    message = (
        r"solve\(callback=...\) argument returned None; will loop "
        "indefinitely unless True is returned."
    )
    with pytest.warns(UserWarning, match=message):
        # Model iterates automatically
        scen.solve(callback=change_distance, **solve_args)

    # Solution reached after 4 iterations, i.e. for d[4 - 1] == 2.5
    assert scen.iteration == 4

    # Fails with invalid arguments
    scen.remove_solution()
    with pytest.raises(ValueError, match="callback='foo' is not callable"):
        scen.solve(**solve_args, callback="foo")
