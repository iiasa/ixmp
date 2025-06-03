"""GitHub adapter for :mod:`sphinx.ext.linkcode`."""
# Expanded from a snippet at https://github.com/sphinx-doc/sphinx/issues/1556

import inspect
import sys
from functools import _lru_cache_wrapper, lru_cache, partial
from pathlib import Path
from types import FunctionType
from typing import TYPE_CHECKING, Optional

from sphinx.util import logging

if TYPE_CHECKING:
    import sphinx.application

log = logging.getLogger(__name__)


def find_remote_head_git(app: "sphinx.application.Sphinx") -> Optional[str]:
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
        refs = list(filter(lambda r: r.commit == commit, remote.refs))  # type: ignore
        if not refs:
            log.info(f"No remote branch for commit {commit}")
            raise ValueError

        # Use the first result, arbitrarily. If the commit hash matches, so will the
        # code.
        return refs[0].remote_head
    except ValueError:  # Either no remote "origin", or raised explicitly
        # Same, but locally
        refs = list(filter(lambda b: b.commit == commit, repo.branches))  # type: ignore
        if not refs:
            log.info(f"Unable to identify a branch for commit {commit}")
            return None

        return refs[0].name


def find_remote_head(app: "sphinx.application.Sphinx") -> str:
    """Return a name for the remote branch containing the code."""
    # Value from configuration
    cfg_remote_head = app.config["linkcode_github_remote_head"]
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
def package_base_path(obj) -> Path:
    """Return the base path of the package containing `obj`."""
    # Module name: obj.__name__ if obj is a module
    module_name = getattr(obj, "__module__", obj.__name__)
    # Path to the top-level package containing the module
    path = sys.modules[module_name.split(".")[0]].__file__
    assert path is not None
    return Path(path).parents[1]


class GitHubLinker:
    """Handler for storing files/line numbers for code objects and formatting links."""

    def __init__(self):
        self.line_numbers = dict()

    def config_inited(self, app: "sphinx.application.Sphinx", config):
        """Handler for the Sphinx ``config-inited`` event."""
        self.base_url = (
            f"https://github.com/{config['linkcode_github_repo_slug']}/blob/"
            + find_remote_head(app)
        )
        log.info(f"linkcode base URL: {self.base_url}")

    def autodoc_process_docstring(
        self, app: "sphinx.application.Sphinx", what, name: str, obj, options, lines
    ):
        """Handler for the Sphinx ``autodoc-process-docstring`` event.

        Records the file and source line numbers containing `obj`.
        """
        # TODO Handle wrapper_descriptor, e.g.
        #      message_ix_models.tests.model.test_bare.TestConfig.__init__

        # Identify the object for which to locate code
        if isinstance(obj, property):
            # Reference the getter method
            obj = obj.fget
        elif isinstance(obj, (FunctionType, _lru_cache_wrapper)):
            # Reference a wrapped function, rather than the wrapper, which may be in the
            # standard library somewhere
            obj = getattr(obj, "__wrapped__", obj)
        elif isinstance(obj, partial):
            # Reference the module in which the partial object is defined
            obj = sys.modules[obj.__module__]
        elif type(obj).__name__ == "FixtureFunctionDefinition":
            # Pytest v8.4 and later. This class is not part of the public API, so check
            # via the class name only
            obj = obj._get_wrapped_function()

        try:
            # Identify the source file and source lines
            sf = inspect.getsourcefile(obj)
            assert sf is not None
            file = Path(sf).relative_to(package_base_path(obj))
            lines, start_line = inspect.getsourcelines(obj)
        except TypeError:
            # inspect.getsourcefile() can't handle ordinary class attributes or
            # module-level data. In linkcode_resolve we'll resolve to the `__init__` or
            # module instead.
            # TODO extend using e.g. ast to identify the source lines
            if what not in {"attribute", "data"}:
                log.error(f"{what=} {name=}, {type(obj).__mro__=}")
                raise
        except Exception as e:  # Other exceptions
            log.info(f"{name} {e}")
        else:
            # Store information for use by linkcode_resolve
            self.line_numbers[name] = (file, start_line, start_line + len(lines))
            # # Display information, for debugging
            # print(name, "→", self.line_numbers[name])

    def linkcode_resolve(self, domain: str, info: dict) -> Optional[str]:
        """Function for the :mod:`sphinx.ext.linkcode` setting of the same name.

        Returns URLs for code objects on GitHub, using information stored by
        :func:`autodoc_process_docstring`.
        """
        # Candidates for lookup in self.line_numbers
        combined = "{module}.{fullname}".format(**info)
        parent = combined.rsplit(".", 1)[0]
        candidates = (combined, parent, f"{parent}.__init__", info["module"])

        try:
            # Use the info for the first of `candidates` available
            line_info: tuple[str, int, int] = next(
                filter(None, map(self.line_numbers.get, candidates))
            )
        except StopIteration:
            log.info(f"Cannot locate code for {combined!r} or parent class/module")
            return None
        else:
            file, start_line, end_line = line_info
            return f"{self.base_url}/{file}#L{start_line}-L{end_line}"


# Single instance
LINKER = GitHubLinker()


def setup(app: "sphinx.application.Sphinx"):
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
    app.config["linkcode_resolve"] = LINKER.linkcode_resolve
