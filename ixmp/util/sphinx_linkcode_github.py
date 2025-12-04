"""GitHub adapter for :mod:`sphinx.ext.linkcode`."""
# Expanded from a snippet at https://github.com/sphinx-doc/sphinx/issues/1556

import inspect
import sys
from collections.abc import Callable, Sequence
from functools import lru_cache, partial
from pathlib import Path
from types import FunctionType, ModuleType
from typing import TYPE_CHECKING, Any

import sphinx.config
from sphinx.util import logging

if TYPE_CHECKING:
    import sphinx.application


log = logging.getLogger(__name__)


def find_remote_head_git(app: "sphinx.application.Sphinx") -> str | None:
    """Use git to identify the name of the remote branch containing the code."""
    try:
        import git
    except ImportError:
        log.info("GitPython not installed; cannot determine remote head")
        return None

    repo = git.Repo(app.srcdir, search_parent_directories=True)

    # Current commit; valid even in detached HEAD state
    commit = repo.head.commit

    try:
        remote = repo.remote("origin")

        # Identify a branch whose head is the same as the current commit
        refs = list(filter(lambda r: r.commit == commit, remote.refs))
        if not refs:
            log.info(f"No remote branch for commit {commit}")
            raise ValueError

        # Use the first result, arbitrarily. If the commit hash matches, so will the
        # code.
        return refs[0].remote_head
    except ValueError:  # Either no remote "origin", or raised explicitly
        # Same, but locally
        refs = list(filter(lambda b: b.commit == commit, repo.branches))
        if not refs:
            log.info(f"Unable to identify a branch for commit {commit}")
            return None

        return refs[0].name


def find_remote_head(app: "sphinx.application.Sphinx") -> str:
    """Return a name for the remote branch containing the code."""
    # Value from configuration
    cfg_remote_head: str = app.config["linkcode_github_remote_head"]
    # Use GitPython to retrieve the repo information
    git_remote_head = find_remote_head_git(app)

    if cfg_remote_head:
        if git_remote_head:
            log.info(
                f"Configuration setting linkcode_github_remote_head={cfg_remote_head} "
                f"overrides value from local git: {git_remote_head}"
            )
        return cfg_remote_head
    elif git_remote_head:
        return git_remote_head
    else:
        raise RuntimeError("Cannot determine a remote head")


@lru_cache()
def package_base_path(
    obj: Callable[[Any], Any] | FunctionType | ModuleType,
) -> Path:
    """Return the base path of the package containing `obj`."""
    # Module name: obj.__name__ if obj is a module
    module_name: str = getattr(obj, "__module__", obj.__name__)
    # Path to the top-level package containing the module
    path = sys.modules[module_name.split(".")[0]].__file__
    assert path is not None
    return Path(path).parents[1]


class GitHubLinker:
    """Handler for storing files/line numbers for code objects and formatting links."""

    line_numbers: dict[str, tuple[Path, int, int]]

    def __init__(self) -> None:
        self.line_numbers = dict()

    def config_inited(
        self, app: "sphinx.application.Sphinx", config: sphinx.config.Config
    ) -> None:
        """Handler for the Sphinx ``config-inited`` event."""
        self.base_url = (
            f"https://github.com/{config['linkcode_github_repo_slug']}/blob/"
            + find_remote_head(app)
        )
        log.info(f"linkcode base URL: {self.base_url}")

    def autodoc_process_docstring(
        self,
        app: "sphinx.application.Sphinx",
        what: str,
        name: str,
        obj: Any,
        options: Any,
        lines: Sequence[str],
    ) -> None:
        """Handler for the Sphinx ``autodoc-process-docstring`` event.

        Records the file and source line numbers containing `obj`.
        """
        # Recursively identify the object for which to locate code
        _obj = obj
        while True:
            type_name = type(_obj).__name__
            if isinstance(_obj, property):
                # property: reference the getter method
                assert _obj.fget
                _obj = _obj.fget
            elif hasattr(_obj, "__wrapped__"):
                # FunctionType, functools._lru_cache_wrapper, others: reference a
                # wrapped function, rather than the wrapper, which may be in the
                # standard library somewhere
                _obj = _obj.__wrapped__
            elif isinstance(_obj, partial):
                # partial: reference the module in which the partial object is defined
                _obj = sys.modules[_obj.__module__]
            elif (
                type_name == "wrapper_descriptor"
                and _obj.__objclass__ is object
                and _obj.__name__ == "__init__"
            ) or type_name == "builtin_function_or_method":
                # Built-in Python objects that getsourcefile() cannot handle
                return
            elif type_name == "FixtureFunctionDefinition":
                # Pytest v8.4 and later. This class is not part of the public API, so
                # check via the class name only
                _obj = _obj._get_wrapped_function()
            else:
                break

        try:
            # Identify the source file and source lines
            sf = inspect.getsourcefile(_obj)
            assert sf is not None
            file = Path(sf).relative_to(package_base_path(_obj))
            lines, start_line = inspect.getsourcelines(_obj)
        except TypeError as e:
            # inspect.getsourcefile() can't handle, inter alia:
            # - ordinary class attributes.
            # - module-level data.
            # - built-in modules.
            #
            # In linkcode_resolve these are resolved to the `__init__` or module
            # instead.
            #
            # TODO extend using e.g. ast to identify the source lines
            if not (what in {"attribute", "data"} or "is a built-in module" in repr(e)):
                log.warning(
                    f"inspect.getsourcefile() failed for {what=} {name=} (MRO: "
                    f"{type(_obj).__mro__}) with {e!r}"
                )
        except Exception as e:  # Other exceptions
            log.warning(f"{name} {e}")
        else:
            # Store information for use by linkcode_resolve
            self.line_numbers[name] = (file, start_line, start_line + len(lines))
            # # Display information, for debugging
            # print(name, "→", self.line_numbers[name])

    def linkcode_resolve(self, domain: str, info: dict[str, str]) -> str | None:
        """Function for the :mod:`sphinx.ext.linkcode` setting of the same name.

        Returns URLs for code objects on GitHub, using information stored by
        :func:`autodoc_process_docstring`.
        """
        # Candidates for lookup in self.line_numbers
        combined = "{module}.{fullname}".format(**info)
        parent = combined.rpartition(".")[0]
        candidates = (combined, parent, f"{parent}.__init__", info["module"])

        # Use the info for the first of `candidates` available
        line_info: list[tuple[Path, int, int]] = list(
            filter(None, map(self.line_numbers.get, candidates))
        )

        if not line_info:
            log.info(f"Cannot locate code for {combined!r} or parent class/module")
            return None
        else:
            file, start_line, end_line = line_info[0]
            return f"{self.base_url}/{file}#L{start_line}-L{end_line}"


# Single instance
LINKER = GitHubLinker()


def setup(app: "sphinx.application.Sphinx") -> dict[str, bool]:
    """Sphinx extension registration hook."""
    # Required first-party extensions
    app.setup_extension("sphinx.ext.autodoc")
    app.setup_extension("sphinx.ext.linkcode")

    # Configuration settings
    app.add_config_value("linkcode_github_repo_slug", None, "")
    app.add_config_value("linkcode_github_remote_head", None, "")

    # Connect signals and config
    app.connect("config-inited", LINKER.config_inited)
    app.connect("autodoc-process-docstring", LINKER.autodoc_process_docstring)
    # Use a lambda to avoid a warning from Sphinx about a direct reference to method
    app.config["linkcode_resolve"] = lambda *args: LINKER.linkcode_resolve(*args)

    return dict(parallel_read_safe=True, parallel_write_safe=True)
