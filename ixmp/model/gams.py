import logging
import os
import re
import shutil
import tempfile
from collections.abc import MutableMapping
from copy import copy
from pathlib import Path
from subprocess import CalledProcessError, check_output, run
from tempfile import TemporaryDirectory
from typing import Any, Optional, Union

from ixmp.backend.common import ItemType
from ixmp.core.scenario import Scenario
from ixmp.util import as_str_list
from ixmp.util.ixmp4 import ContainerData

from .base import Model, ModelError

log = logging.getLogger(__name__)

# Singleton instance of GAMSInfo.
_GAMS_INFO: Optional["GAMSInfo"] = None


#: Return codes used by GAMS, from
#: https://www.gams.com/latest/docs/UG_GAMSReturnCodes.html . Values over 256 are only
#: valid on Windows, and are returned modulo 256 on other platforms.
RETURN_CODE = {
    0: "Normal return",
    1: "Solver is to be called, the system should never return this number",
    2: "There was a compilation error",
    3: "There was an execution error",
    4: "System limits were reached",
    5: "There was a file error",
    6: "There was a parameter error",
    7: "There was a licensing error",
    8: "There was a GAMS system error",
    9: "GAMS could not be started",
    10: "Out of memory",
    11: "Out of disk",
    109: "Could not create process/scratch directory",
    110: "Too many process/scratch directories",
    112: "Could not delete the process/scratch directory",
    113: "Could not write the script gamsnext",
    114: "Could not write the parameter file",
    115: "Could not read environment variable",
    400: "Could not spawn the GAMS language compiler (gamscmex)",
    401: "Current directory (curdir) does not exist",
    402: "Cannot set current directory (curdir)",
    404: "Blank in system directory",
    405: "Blank in current directory",
    406: "Blank in scratch extension (scrext)",
    407: "Unexpected cmexRC",
    408: "Could not find the process directory (procdir)",
    409: "CMEX library not be found (experimental)",
    410: "Entry point in CMEX library could not be found (experimental)",
    411: "Blank in process directory",
    412: "Blank in scratch directory",
    909: "Cannot add path / unknown UNIX environment / cannot set environment variable",
    1000: "Driver error: incorrect command line parameters for gams",
    2000: "Driver error: internal error: cannot install interrupt handler",
    3000: "Driver error: problems getting current directory",
    4000: "Driver error: internal error: GAMS compile and execute module not found",
    5000: "Driver error: internal error: cannot load option handling library",
}
RETURN_CODE = {key % 256: value for key, value in RETURN_CODE.items()}


class GAMSInfo:
    """Information about the GAMS installation."""

    # Optional environment variable with the path to a directory containing the GAMS
    # executable
    _env: str = "IXMP_GAMS_PATH"
    # Name of the GAMS executable
    _name: str = "gams"

    #: GAMS version as a string, for instance "24.7.4". If the GAMS system installation
    #: cannot be located, the string contains an informative error message.
    version: str

    #: GAMS system directory.
    system_dir: Path

    def __init__(self) -> None:
        # Retrieve some `output` containing GAMS installation info
        with TemporaryDirectory() as temp_dir:
            # NB the following do not work:
            # - check_output(['gams'], ...) —because GAMS writes directly to the console
            #   instead of to stdout.
            # - check_output(['gams', '-LogOption=3'], ...) —because GAMS does not
            #   accept options without an input file to execute.
            # …so instead create a GAMS source file that does nothing:
            Path(temp_dir, "null.gms").write_text("$exit;")

            try:
                # Execute this no-op file and capture stdout
                args = [self.executable, "null.gms", "-LogOption=3"]
                output = check_output(
                    args, cwd=temp_dir, shell=os.name == "nt", universal_newlines=True
                )
            except Exception:
                output = ""

        # Parse GAMS version from the copyright line
        if match := re.search(r"^GAMS ([\d\.]+)\s*Copyright", output, re.MULTILINE):
            self.version = match.group(1)
        else:
            self.version = (
                f"no {self._name!r} executable in {self._env}="
                f"{os.getenv(self._env, '(not set)')} or the system PATH"
            )

        # Parse GAMS system directory path
        if match := re.search(r"^\s*SysDir (.*)", output, re.MULTILINE):
            self.system_dir = Path(match.group(1))
        else:
            self.system_dir = Path.cwd()

    @property
    def executable(self) -> str:
        """Return the path to a GAMS executable.

        This seeks a program :program:`gams` using :func:`shutil.which`. The program is
        sought in the the directory given by the ``IXMP_GAMS_PATH`` environment variable
        (if set), or else the system PATH.
        """
        which_path = os.getenv(self._env)  # None if not set
        if path := shutil.which(self._name, path=which_path):
            return path
        elif which_path is None:
            return self._name
        else:
            return str(Path(which_path, self._name))

    @property
    def java_api_dir(self) -> Path:
        """Java API files subdirectory of :attr:`.system_dir`."""
        return self.system_dir.joinpath("apifiles", "Java", "api")


class GAMSModel(Model):
    """Generic base class for :mod:`ixmp` models using `GAMS <https://gams.com>`_.

    GAMSModel solves a :class:`.Scenario` using the following steps:

    1. All Scenario data is written to a model input file in GDX format.
    2. A GAMS program is run to perform calculations, producing output in a GDX file.
    3. Output, or solution, data is read from the GDX file and stored in the Scenario.

    When created and :meth:`run`, GAMSModel constructs file paths and other necessary
    values using format strings. The :attr:`defaults` may be overridden by the keyword
    arguments to the constructor:

    Other parameters
    ----------------
    name : str, optional
        Override the :attr:`name` attribute to provide the `model_name` for format
        strings.
    model_file : str, optional
        Path to GAMS file, including :file:`.gms` extension. Default:
        :file:`{model_name}.gms` in the current directory.
    case : str, optional
        Run or case identifier to use in GDX file names. Default:
        ``'{scenario.model}_{scenario.name}'``, where `scenario` is the
        :class:`.Scenario` object passed to :meth:`run`. Formatted using `model_name`
        and `scenario`.
    in_file : str, optional
        Path to write GDX input file. Default: ``'{model_name}_in.gdx'``. Formatted
        using `model_name`, `scenario`, and `case`.
    out_file : str, optional
        Path to read GDX output file. Default: ``'{model_name}_out.gdx'``. Formatted
        using `model_name`, `scenario`, and `case`.
    solve_args : list of str, optional
        Arguments to be passed to GAMS, e.g. to identify the model input and output
        files. Each formatted using `model_file`, `scenario`, `case`, `in_file`, and
        `out_file`. Default:

        - ``'--in="{in_file}"'``
        - ``'--out="{out_file}"'``
    gams_args : list of str, optional
        Additional arguments passed directly to GAMS without formatting, e.g. to
        control solver options or behaviour. See the `GAMS Documentation <https://www.gams.com/latest/docs/UG_GamsCall.html#UG_GamsCall_ListOfCommandLineParameters>`_.
        For example:

        - :py:`gams_args=["iterLim=10"]` limits the solver to 10 iterations.
    quiet: bool, optional
        If :obj:`True`, add "LogOption=2" to `gams_args` to redirect most console output
        during the model run to the log file. Default :obj:`False`, so "LogOption=4" is
        added. Any "LogOption" value provided explicitly via `gams_args` takes
        precedence.
    check_solution : bool, optional
        If :obj:`True`, raise an exception if the GAMS solver did not reach optimality.
        (Only for MESSAGE-scheme Scenarios.)
    comment : str, optional
        Comment added to Scenario when importing the solution. If omitted, no comment is
        added.
    equ_list : list of str, optional
        Equations to be imported from the `out_file`. Default: all.
    var_list : list of str, optional
        Variables to be imported from the `out_file`. Default: all.
    record_version_packages : list of str, optional
        Names of Python packages to record versions. Default: :py:`["ixmp"]`.
        See :meth:`record_versions`.
    container_data : list of :class:`ixmp.util.ixmp4.ContainerData`, optional
        List of data to add to the GAMS Container used by the IXMP4Backend for GAMS I/O.
        Default: empty list.
    """  # noqa: E501

    # Make attributes known to self
    model_file: str
    case: str
    in_file: os.PathLike
    out_file: os.PathLike
    solve_args: list[str]
    gams_args: list[str]
    check_solution: bool
    comment: Optional[str]
    equ_list: Optional[list[str]]
    var_list: Optional[list[str]]
    quiet: bool
    use_temp_dir: bool
    record_version_packages: list[str]
    container_data: list[ContainerData]

    #: Model name.
    name = "default"

    #: Default values and format strings for options.
    defaults: MutableMapping[str, Any] = {
        "model_file": "{model_name}.gms",
        "case": "{scenario.model}_{scenario.scenario}",
        "in_file": str(Path("{cwd}", "{model_name}_in.gdx")),
        "out_file": str(Path("{cwd}", "{model_name}_out.gdx")),
        "solve_args": ['--in="{in_file}"', '--out="{out_file}"'],
        # Not formatted
        "gams_args": [],
        "check_solution": True,
        "comment": None,
        "equ_list": None,
        "var_list": None,
        "quiet": False,
        "use_temp_dir": True,
        "record_version_packages": ["ixmp"],
        "container_data": [],
    }

    def __init__(self, name_=None, **model_options):
        self.model_name = self.clean_path(name_ or self.name)

        # Store options from `model_options`, otherwise from `defaults`
        for arg_name, default in self.defaults.items():
            setattr(self, arg_name, model_options.get(arg_name, copy(default)))

        # Check whether a subclass or user already set LogOption in `gams_args`
        if not any("LogOption" in arg for arg in self.gams_args):
            # Not set; use `quiet` to determine the value
            self.gams_args.append(f"LogOption={'2' if self.quiet else '4'}")

    def format_exception(
        self, exc: Exception, model_file: Path, backend_class: type
    ) -> Exception:
        """Format a user-friendly exception when GAMS errors."""
        log_file = Path(self.cwd).joinpath(model_file.name).with_suffix(".log")
        lst_file = Path(self.cwd).joinpath(model_file.name).with_suffix(".lst")
        lp_5 = "LP status (5): optimal with unscaled infeasibilities"

        if rc := getattr(exc, "returncode", 0):
            # Convert a Windows return code >256 to its POSIX equivalent
            msg = [
                f"GAMS errored with return code {rc}:",
                f"    {RETURN_CODE[rc % 256]}",
            ]
        elif lst_file.exists() and lp_5 in lst_file.read_text():  # pragma: no cover
            msg = [
                "GAMS returned 0 but indicated:",
                f"    {lp_5}",
                f"and {backend_class.__name__} could not read the solution.",
            ]
        else:  # pragma: no cover
            return exc  # Other exception

        # Add a reference to the listing and log files, if they exist
        msg.extend(
            ["", "For details, see the terminal output above, plus:"]
            + ([f"Listing   : {lst_file}"] if lst_file.exists() else [])
            + ([f"Log file  : {log_file}"] if log_file.exists() else [])
            + [f"Input data: {self.in_file}"]
        )

        return ModelError("\n".join(msg))

    def format_option(self, name):
        """Retrieve the option `name` and format it."""
        return self.format(getattr(self, name))

    def format(self, value):
        """Helper for recursive formatting of model options.

        `value` is formatted with replacements from the attributes of `self`.
        """
        try:
            return value.format(**self.__dict__)
        except AttributeError:
            # Something like a Path; don't format it
            return value

    def record_versions(self):
        """Store Python package versions as set elements to be written to GDX.

        The values are stored in a 2-dimensional set named ``ixmp_version``, where the
        first element is the package name, and the second is its version according to
        :func:`importlib.metadata.version`). If the package is not installed, the
        string "(not installed)" is stored.
        """
        from importlib.metadata import PackageNotFoundError, version

        name = "ixmp_version"
        with self.scenario.transact():
            try:
                # Initialize the set
                self.scenario.init_set(name, ["*", "*"], ["package", "version"])
            except ValueError as e:  # pragma: no cover
                # NB this will only occur if the user directly creates the set; the one
                #    created here is deleted in run()
                if "already exists" not in e.args[0]:
                    raise

            # Handle each identified package
            for package in self.record_version_packages:
                try:
                    # Retrieve the version; replace characters not supported by GAMS
                    package_version = version(package).replace(".", "-")
                except PackageNotFoundError:
                    package_version = "(not installed)"  # Not installed
                # Add to the set
                self.scenario.add_set(name, (package, package_version))

    def remove_temp_dir(self, msg="after run()"):
        """Remove the temporary directory, if any."""
        try:
            if self.use_temp_dir and self.cwd.exists():
                shutil.rmtree(self.cwd)
        except AttributeError:
            pass  # No .cwd, e.g. in a subclass
        except PermissionError as e:
            log.debug(f"Could not delete {repr(self.cwd)} {msg}")
            log.debug(str(e))

    def __del__(self):
        # Try once more to remove the temporary directory.
        # This appears to still fail on Windows.
        self.remove_temp_dir("at GAMSModel teardown")

    def run(self, scenario: Scenario) -> None:
        """Execute the model.

        Among other steps:

        - :meth:`record_versions` is called.
        - Data is written to a GDX file using the associated :class:`.Backend`.
        - The ``ixmp_version`` set created by :meth:`record_versions` is deleted.
        - :program:`gams` is invoked to execute the model file.
        - If :program:`gams` completes successfully:

          - GAMS output/model solution data is read from a GDX file.
        """
        from ixmp.backend.jdbc import JDBCBackend

        # Store the scenario so its attributes can be referenced by format()
        self.scenario = scenario

        # NOTE workaround delattr(self, "scenario"); differentiate backend types without
        # moving that call
        # If we get more backend types, we'll need to adjust this
        backend_type = (
            "jdbc"
            if isinstance(self.scenario.platform._backend, JDBCBackend)
            else "ixmp4"
        )

        if backend_type == "jdbc":
            # Record versions of packages listed in `record_version_packages`
            self.record_versions()

        # Format or retrieve the model file option
        model_file = Path(self.format_option("model_file"))

        # Determine working directory for the GAMS call, possibly a temporary directory
        self.cwd = Path(tempfile.mkdtemp()) if self.use_temp_dir else model_file.parent
        # The "case" name
        self.case = self.clean_path(self.format_option("case").replace(" ", "_"))
        # Input and output file names
        self.in_file = Path(self.format_option("in_file"))
        self.out_file = Path(self.format_option("out_file"))

        # Assemble the full command: executable, model file, model-specific arguments,
        # and general GAMS arguments
        command: Union[str, list[str]] = (
            ["gams", f'"{model_file}"']
            + [self.format(arg) for arg in self.solve_args]
            + self.gams_args
        )

        if os.name == "nt":
            # Windows: join the commands to a single string
            command = " ".join(command)

        # Remove stored reference to the Scenario to allow it to be GC'd later
        delattr(self, "scenario")

        # Common argument for write_file and read_file
        s_arg: dict[str, object] = dict(filters=dict(scenario=scenario))

        # Instruct ixmp4 to record package versions
        if backend_type == "ixmp4":
            s_arg["record_version_packages"] = self.record_version_packages
            s_arg["container_data"] = self.container_data

        try:
            # Write model data to file
            scenario.platform._backend.write_file(
                self.in_file, ItemType.SET | ItemType.PAR, **s_arg
            )
        except NotImplementedError:  # pragma: no cover
            # No coverage because there currently is no such Backend that doesn't
            # support GDX

            # Remove the temporary directory, which should be empty
            self.remove_temp_dir()

            raise NotImplementedError(
                "GAMSModel requires a Backend that can write to GDX files, e.g. "
                "JDBCBackend"
            )
        else:
            if backend_type == "jdbc":
                # Remove ixmp_version set entirely
                with scenario.transact():
                    scenario.remove_set("ixmp_version")
            else:  # backend_type == "ixmp4"
                # Clean up s_arg for use in read_file()
                s_arg.pop("record_version_packages")
                s_arg.pop("container_data")
        try:
            # Invoke GAMS
            run(command, shell=os.name == "nt", cwd=self.cwd, check=True)

            # Read model solution
            scenario.platform._backend.read_file(
                self.out_file,
                ItemType.MODEL,
                **s_arg,
                check_solution=self.check_solution,
                comment=self.comment or "",
                equ_list=as_str_list(self.equ_list) or [],
                var_list=as_str_list(self.var_list) or [],
            )
        except (CalledProcessError, RuntimeError) as exc:
            # CalledProcessError from run(); RuntimeError from read_file()
            # Do not remove self.temp_dir; the user may want to inspect the GDX file
            raise self.format_exception(
                exc, model_file, scenario.platform._backend.__class__
            ) from None

        # Finished: remove the temporary directory, if any
        self.remove_temp_dir()


def gams_info() -> GAMSInfo:
    """Return an instance of :class:`.GAMSInfo`."""
    # Singleton pattern; ensure there is only one instance of GAMSInfo
    global _GAMS_INFO

    if _GAMS_INFO is None:
        # Create the singleton
        _GAMS_INFO = GAMSInfo()

    return _GAMS_INFO


def gams_version() -> Optional[str]:
    """Return :attr:`.GAMSInfo.version`."""
    return gams_info().version
