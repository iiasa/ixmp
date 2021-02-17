import os
import re
from pathlib import Path
from subprocess import CalledProcessError, check_call
from tempfile import TemporaryDirectory
from typing import Mapping

from ixmp.backend import ItemType
from ixmp.model.base import Model, ModelError
from ixmp.utils import as_str_list


def gams_version():
    """Return the GAMS version as a string, e.g. '24.7.4'."""
    # NB check_output(['gams'], ...) does not work, because GAMS writes
    #    directly to the console instead of to stdout.
    #    check_output(['gams', '-LogOption=3'], ...) does not work, because
    #    GAMS does not accept options without an input file to execute.
    import os
    from subprocess import check_output
    from tempfile import mkdtemp

    # Create a temporary GAMS program that does nothing
    tmp_dir = Path(mkdtemp())
    gms = tmp_dir / "null.gms"
    gms.write_text("$exit;")

    # Execute, capturing stdout
    output = check_output(
        ["gams", "null", "-LogOption=3"],
        shell=os.name == "nt",
        cwd=tmp_dir,
        universal_newlines=True,
    )

    # Clean up
    gms.unlink()
    gms.with_suffix(".lst").unlink()
    tmp_dir.rmdir()

    # Find and return the version string
    pattern = r"^GAMS ([\d\.]+)\s*Copyright"
    return re.search(pattern, output, re.MULTILINE).groups()[0]


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


class GAMSModel(Model):
    """General class for ixmp models using `GAMS <https://gams.com/>`_.

    GAMSModel solves a Scenario using the following steps:

    1. All Scenario data is written to a model input file in GDX format.
    2. A GAMS program is run to perform calculations, producing output in a
       GDX file.
    3. Output, or solution, data is read from the GDX file and stored in the
       Scenario.

    When created and :meth:`run`, GAMSModel constructs file paths and other
    necessary values using format strings. The :attr:`defaults` may be
    overridden by the keyword arguments to the constructor:

    Other parameters
    ----------------
    name : str, optional
        Override the :attr:`name` attribute to provide the `model_name` for
        format strings.
    model_file : str, optional
        Path to GAMS file, including '.gms' extension. Default: ``'{model_name}.gms'``
        in the current directory.
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

        - ``'LogOption=4'`` prints output to stdout (not console) and the log file.
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
    """  # noqa: E501

    #: Model name.
    name = "default"

    #: Default model options:
    defaults: Mapping[str, object] = {
        "model_file": "{model_name}.gms",
        "case": "{scenario.model}_{scenario.scenario}",
        "in_file": str(Path("{cwd}", "{model_name}_in.gdx")),
        "out_file": str(Path("{cwd}", "{model_name}_out.gdx")),
        "solve_args": ['--in="{in_file}"', '--out="{out_file}"'],
        # Not formatted
        "gams_args": ["LogOption=4"],
        "check_solution": True,
        "comment": None,
        "equ_list": None,
        "var_list": None,
        "use_temp_dir": True,
    }

    def __init__(self, name_=None, **model_options):
        self.model_name = self.clean_path(name_ or self.name)

        for arg_name, default in self.defaults.items():
            setattr(self, arg_name, model_options.get(arg_name, default))

    def format_exception(self, exc, model_file):
        """Format a user-friendly exception when GAMS errors."""
        msg = [
            f"GAMS errored with return code {exc.returncode}:",
            # Convert a Windows return code >256 to its equivalent on *nix platforms
            f"    {RETURN_CODE[exc.returncode % 256]}",
            "",
            "For details, see the terminal output above, plus:",
            f"Input data: {self.in_file}",
        ]

        # Add a reference to the listing file, if it exists
        lst_file = Path(self.cwd).joinpath(model_file.name).with_suffix(".lst")
        if lst_file.exists():
            msg.insert(-1, f"Listing   : {lst_file}")

        return ModelError("\n".join(msg))

    def format_option(self, name):
        """Retrieve the option `name` and format it."""
        return self.format(getattr(self, name))

    def format(self, value):
        """Helper for recursive formatting of model options."""
        try:
            return value.format(**self.__dict__)
        except AttributeError:
            # Something like a Path; don't format it
            return value

    def run(self, scenario):
        """Execute the model."""
        # Store the scenario so its attributes can be referenced by format()
        self.scenario = scenario

        if self.use_temp_dir:
            # Create a temp directory; automatically deleted at the end of this method
            _temp_dir = TemporaryDirectory()
            self.temp_dir = _temp_dir.name

        # Process args in order to assemble the full command
        command = ["gams"]

        model_file = Path(self.format_option("model_file"))
        command.append(f'"{model_file}"')

        # Now can determine the current working directory
        self.cwd = self.temp_dir if self.use_temp_dir else model_file.parent
        # The "case" name
        self.case = self.format_option("case").replace(" ", "_")
        # Input and output file names
        self.in_file = Path(self.format_option("in_file"))
        self.out_file = Path(self.format_option("out_file"))

        # Add model-specific arguments
        command.extend(self.format(arg) for arg in self.solve_args)
        # General GAMS arguments
        command.extend(self.gams_args)

        if os.name == "nt":
            # Windows: join the commands to a single string
            command = " ".join(command)

        # Common argument for write_file and read_file
        s_arg = dict(filters=dict(scenario=scenario))
        try:
            # Write model data to file
            scenario.platform._backend.write_file(
                self.in_file, ItemType.SET | ItemType.PAR, **s_arg
            )
        except NotImplementedError:  # pragma: no cover
            # Currently there is no such Backend
            raise NotImplementedError(
                "GAMSModel requires a Backend that can write to GDX files, e.g. "
                "JDBCBackend"
            )

        try:
            # Invoke GAMS
            check_call(command, shell=os.name == "nt", cwd=self.cwd)
        except CalledProcessError as exc:
            raise self.format_exception(exc, model_file) from None

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
