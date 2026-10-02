import cProfile
import pstats
from collections.abc import Callable, Generator
from contextlib import _GeneratorContextManager, contextmanager
from pathlib import Path
from typing import Any, TypeAlias

import pytest

from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.testing.data import make_dantzig


@pytest.fixture(scope="function")
def profiled(
    request: pytest.FixtureRequest,
) -> Generator[Callable[[], _GeneratorContextManager[None]]]:
    """Use this fixture for profiling tests:
    ```
    def test(profiled):
        # setup() ...
        with profiled():
            complex_procedure()
        # teardown() ...
    ```
    Profiler output will be written to '.profiles/{testname}.prof'
    """

    testname = request.node.name
    pr = cProfile.Profile()

    @contextmanager
    def profiled() -> Generator[None, Any, None]:
        pr.enable()
        yield
        pr.disable()

    yield profiled
    ps = pstats.Stats(pr)
    Path(".profiles").mkdir(parents=True, exist_ok=True)
    # NOTE Install e.g. snakeviz to visualize these
    ps.dump_stats(f".profiles/{testname}.prof")


Profiled: TypeAlias = Callable[[], _GeneratorContextManager[None]]


class TestScenarioClone:
    @pytest.mark.jdbc
    def test_scenario_clone_jdbc_same_mp(
        self, test_mp_f: Platform, profiled: Profiled, benchmark: Any
    ) -> None:
        def setup() -> tuple[tuple[()], dict[str, object]]:
            scenario = make_dantzig(mp=test_mp_f, solve=True, quiet=True)
            return (), {"scenario": scenario}

        def run(scenario: Scenario) -> None:
            with profiled():
                for i in range(10):
                    scenario.clone(model=f"Model_{i}")

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.ixmp4
    def test_scenario_clone_ixmp4_same_mp(
        self, test_mp_f: Platform, profiled: Profiled, benchmark: Any
    ) -> None:
        def setup() -> tuple[tuple[()], dict[str, object]]:
            scenario = make_dantzig(mp=test_mp_f, solve=True, quiet=True)
            return (), {"scenario": scenario}

        def run(scenario: Scenario) -> None:
            with profiled():
                for i in range(10):
                    scenario.clone(model=f"Model_{i}")

        benchmark.pedantic(run, setup=setup)


# Enable these tests by commenting out --benchmark-skip in pyproject.toml

### TODO
### Lessons learned:
### scenario.__init__() /dantzig.initialize() is much slower since ixmp4 uses lazy loading and doesn't cache objects here (even though it inherits from CacheBackend!)
### A significant amount of time is also spent on tabulating data/items and adding them back in one by one in ixmp4. So try this:
### 1. Set relationships to eager loading (or if sqla doesn't recommend that, use eager loading only so that data is present when Run is created before cloning?!)
### 2. Make sure IXMP4Backend uses CachingBackend correctly
###     - Only item_get_elements stores values in the cache
###     - How is it, then, that it seems that the jdbc clone doesn't seem to need to load data?
###     - Support timeseries.preload() function?
###         - This implies not setting the whole iamc relationship to eager loading!
###         - This seems to only be used in tests
###     - Support scenario.load_scenario_data() function?
###         - This iterates over all items and loads the data if every individual item
###         - This seems to only be used in tests
### 3. Revise ixmp4 clone function to use a single query or at least a single transaction block, if possible, handling all kinds of data at once
