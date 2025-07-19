import logging
import re
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
import pytest

# TODO Use "from typing import Unpack" when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp import Scenario
from ixmp.model.base import Model, ModelError
from ixmp.model.dantzig import DantzigModel
from ixmp.model.gams import GAMSInfo, GAMSModel, gams_version
from ixmp.testing import assert_logs, make_dantzig

if TYPE_CHECKING:
    from ixmp.core.platform import Platform
    from ixmp.types import GamsModelInitKwargs


def test_base_model() -> None:
    # An incomplete Backend subclass can't be instantiated
    class M1(Model):
        pass

    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class M1 with(out an implementation for)? "
        "abstract methods",
    ):
        M1(name_="test")  # type: ignore[abstract]


# FIXME IXMP4Backend doesn't handle scenario.change_scalar() correctly:
# We can't just create() it, it might already exist, so then we need update()
@pytest.mark.jdbc
def test_model_initialize(
    test_mp: "Platform",
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> None:
    # Model.initialize runs on an empty Scenario
    s = make_dantzig(test_mp, request=request)
    b1 = s.par("b")
    assert len(b1) == 3

    # Modify a value for 'b'
    s.check_out()
    new_value = 301
    s.add_par("b", "chicago", new_value, "cases")
    s.commit("Overwrite b(chicago)")

    # Model.initialize runs on an already-initialized Scenario, without error
    DantzigModel.initialize(s, with_data=True)

    # Data has the same length...
    b2 = s.par("b")
    assert len(b2) == 3
    # ...but modified value(s) are not overwritten
    assert isinstance(b2, pd.DataFrame)
    assert (b2.query("j == 'chicago'")["value"] == new_value).all()

    # Unrecognized Scenario(scheme=...) is initialized using the base method, a
    # no-op
    messages = [
        "No scheme for new Scenario model-name/scenario-name",
        "No initialization for None-scheme Scenario",
    ]
    with assert_logs(caplog, messages, at_level=logging.DEBUG):
        Scenario(test_mp, model="model-name", scenario="scenario-name", version="new")

    with assert_logs(
        caplog, "No initialization for 'foo'-scheme Scenario", at_level=logging.DEBUG
    ):
        Scenario(
            test_mp,
            model="model-name",
            scenario="scenario-name",
            version="new",
            scheme="foo",
        )

    # Keyword arguments to Scenario(...) that are not recognized by
    # Model.initialize() raise an intelligible exception
    with pytest.raises(TypeError, match="unexpected keyword argument 'bad_arg1'"):
        Scenario(
            test_mp,
            model="model-name",
            scenario="scenario-name",
            version="new",
            scheme="unknown",
            bad_arg1=111,  # type: ignore[call-arg]
        )

    with pytest.raises(TypeError, match="unexpected keyword argument 'bad_arg2'"):
        Scenario(
            test_mp,
            model="model-name",
            scenario="scenario-name",
            version="new",
            scheme="dantzig",
            with_data=True,
            bad_arg2=222,  # type: ignore[call-arg]
        )

    # Replace b[j] with a parameter of the same name, but different indices
    s.check_out()
    s.remove_par("b")
    s.init_par("b", idx_sets=["i"], idx_names=["i_dim"])

    # Logs an error message
    with assert_logs(caplog, "Existing index sets of 'b' ('i',) do not match ('j',)"):
        DantzigModel.initialize(s)


def test_gams_version() -> None:
    parts = gams_version().split(".")

    # Returns a version string like X.Y.Z, in which each part is a number (no leading or
    # trailing text)
    assert 3 == len(parts)
    assert [int(p) for p in parts]


class TestGAMSInfo:
    def test_version(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """:attr:`GAMSInfo.version` contains useful info if no installation found."""
        # Set the expected name of the GAMS executable to a non-existent program
        monkeypatch.setattr(GAMSInfo, "_name", "__F_O_O")

        # GAMSInfo.executable() runs as called from GAMSInfo.__init__()
        gi1 = GAMSInfo()

        # GAMSInfo.version contains an instructive message
        msg = "no '__F_O_O' executable in IXMP_GAMS_PATH={} or the system PATH"
        assert msg.format("(not set)") == gi1.version

        # GAMSInfo.version picks up the IXMP_GAMS_PATH environment variable
        monkeypatch.setenv("IXMP_GAMS_PATH", str(tmp_path))
        gi2 = GAMSInfo()
        assert msg.format(str(tmp_path)) == gi2.version


class TestGAMSModel:
    @pytest.fixture(scope="class")
    def dantzig(
        self, test_mp: "Platform", request: pytest.FixtureRequest
    ) -> Generator[Scenario, Any, None]:
        yield make_dantzig(test_mp, request=request)

    @pytest.mark.parametrize("char", r'<>"/\|?*')
    def test_filename_invalid_char(self, dantzig: Scenario, char: str) -> None:
        """Model can be solved with invalid character names."""
        name = f"foo{char}bar"
        s = dantzig.clone(model=name, scenario=name)

        # Indirectly test backend.write_file("….gdx")
        # This name_ keyword argument ends up received to GAMSModel.__init__ and sets
        # the GAMSModel.model_name attribute, and in turn the GDX file names used.
        s.solve(name_=name, quiet=True)

    # FIXME IXMP4Backend should support this
    @pytest.mark.jdbc
    @pytest.mark.parametrize(
        "kwargs",
        [
            dict(comment=None),
            dict(equ_list=None, var_list=["x"]),
            dict(equ_list=["demand", "supply"], var_list=[]),
        ],
        ids=["null-comment", "null-list", "empty-list"],
    )
    def test_GAMSModel_solve(
        self,
        test_data_path: Path,
        dantzig: Scenario,
        kwargs: "GamsModelInitKwargs",
    ) -> None:
        """Options to GAMSModel are handled without error."""
        kwargs["quiet"] = True
        dantzig.clone().solve(**kwargs)

    def test_error_message(self, test_data_path: Path, test_mp: "Platform") -> None:
        """GAMSModel.solve() displays a user-friendly message on error."""
        # Empty Scenario
        s = Scenario(test_mp, model="foo", scenario="bar", version="new")
        s.commit("Initial commit")

        # Expected paths for error message
        paths = map(
            lambda name: re.escape(str(test_data_path.joinpath(name))),
            ["_abort.lst", "_abort.log", "default_in.gdx"],
        )

        with pytest.raises(
            ModelError,
            match="""GAMS errored with return code 2:
    There was a compilation error

For details, see the terminal output above, plus:
Listing   : {}
Log file  : {}
Input data: {}""".format(*paths),
        ):
            s.solve(model_file=test_data_path / "_abort.gms", use_temp_dir=False)

    def test_subclass_init(self) -> None:
        """Subclasses can call :py:`super().__init__()`, as in :mod:`message_ix`."""

        class Foo(GAMSModel):
            def __init__(
                self,
                name: Optional[str] = None,
                **model_options: Unpack["GamsModelInitKwargs"],
            ) -> None:
                super().__init__(name, **model_options)

        Foo()
