"""GitHub integration for sphinx.ext.linkcode.

Expanded from https://github.com/sphinx-doc/sphinx/issues/1556.
"""

import inspect
from pathlib import Path
from typing import TYPE_CHECKING

from sphinx.util import logging

if TYPE_CHECKING:
    import sphinx.application


def _git_remote_head(app: "sphinx.application.Sphinx", log) -> str:
    """Use git to identify the name of the remote branch containing the code."""
    import git

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
    except ValueError:
        # Same, but locally
        refs = list(filter(lambda b: b.commit == commit, repo.branches))  # type: ignore
        if not refs:
            raise ValueError(f"Unable to identify a branch for commit {commit}")

    # Use the first result, arbitrarily. If the commit hash matches, so will the code.
    return refs[0].name


def remote_head(app: "sphinx.application.Sphinx", log) -> str:
    """Return a name for the remote branch containing the code."""
    result = None
    try:
        # Use GitPython to retrieve the repo information
        result = _git_remote_head(app, log)
    except (ImportError, RuntimeError) as e:
        log.info(e)

    # Use value from configuration
    from_cfg = app.config["linkcode_github_remote_head"]
    if from_cfg and remote_head:
        log.info(
            f"Configuration setting linkcode_github_remote_head={from_cfg}"
            f"overrides value from local git: {result}"
        )
        result = from_cfg

    assert result is not None

    return result


class GitHubLinker:
    def __init__(self):
        self.line_numbers = dict()
        self.log = logging.getLogger(__name__)

    def config_inited(self, app: "sphinx.application.Sphinx", config):
        self.file_root = Path(app.srcdir).parent
        self.base_url = (
            f"https://github.com/{config['linkcode_github_repo_slug']}/blob/"
            + remote_head(app, self.log)
        )

    def autodoc_process_docstring(
        self, app: "sphinx.application.Sphinx", what, name: str, obj, options, lines
    ):
        """Retrieve file names and line numbers.

        Misuse the autodoc hook because we have access to the actual object here.
        """
        # We can't properly handle ordinary attributes.
        # In linkcode_resolve we'll resolve to the `__init__` or module instead
        if what == "attribute":
            return
        elif hasattr(obj, "fget"):
            # Special casing for properties
            obj = obj.fget

        try:
            sf = inspect.getsourcefile(obj)
            if sf:
                file = Path(sf).relative_to(self.file_root)
                source_lines, start_line = inspect.getsourcelines(obj)
                end_line = start_line + len(source_lines)
                self.line_numbers[name] = (file, start_line, end_line)
        except Exception:
            pass

        # In case `__init__` is not documented, we call this manually to have it
        # available for attributes -- see the note above
        if what == "class":
            self.autodoc_process_docstring(
                app, "method", f"{name}.__init__", obj.__init__, options, lines
            )

    def linkcode_resolve(self, domain, info):
        """See www.sphinx-doc.org/en/master/usage/extensions/linkcode.html."""
        combined = "{module}.{fullname}".format(**info)

        for candidate in (
            combined,
            f"{combined.rsplit('.', 1)[0]}.__init__",  # __init__
            f"{combined.rsplit('.', 1)[0]}",  # Class
            info["module"],  # Module
        ):
            line_info = self.line_numbers.get(candidate)
            if line_info:
                self.log.debug(f"Cannot find a code link for {combined!r}")
                break

        if not line_info:
            return

        file, start_line, end_line = line_info
        return f"{self.base_url}/{line_info[0]}#L{start_line}-L{end_line}"


LINKER = GitHubLinker()


def setup(app: "sphinx.application.Sphinx"):
    app.setup_extension("sphinx.ext.autodoc")
    app.setup_extension("sphinx.ext.linkcode")

    app.add_config_value("linkcode_github_repo_slug", None, "")
    app.add_config_value("linkcode_github_remote_head", None, "")

    app.connect("config-inited", LINKER.config_inited)
    app.connect("autodoc-process-docstring", LINKER.autodoc_process_docstring)
    app.config["linkcode_resolve"] = LINKER.linkcode_resolve
