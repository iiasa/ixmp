import re
from pathlib import Path

from ixmp.backend.jdbc.options import DRIVER, Options


class TestOptions:
    def test_default_table_types_duplicate(self, tmp_path: Path) -> None:
        """ "hsqldb.default_table_type=cached" is added by default, at most once."""
        expr = re.compile("jdbc:hsqldb:file:[^;]*;hsqldb.default_table_type=cached")

        # Using a path
        assert expr.fullmatch(Options(DRIVER.hsqldb, path=tmp_path).full_url)

        # Using a URL
        assert expr.fullmatch(Options(DRIVER.hsqldb, url="file:foo").full_url)

        # Using a URL with the parameter already added
        opt = Options(DRIVER.hsqldb, url="file:foo;hsqldb.default_table_type=cached")
        assert expr.fullmatch(opt.full_url)

        # Existing property with default value is not overridden
        opt = Options(DRIVER.hsqldb, url="file:foo;hsqldb.default_table_type=memory")
        assert not expr.fullmatch(opt.full_url) and "=cached" not in opt.full_url
