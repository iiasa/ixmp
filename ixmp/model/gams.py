import os
import re
from pathlib import Path
from subprocess import check_call
from tempfile import TemporaryDirectory
from typing import Mapping

from ixmp.backend import ItemType
from ixmp.model.base import Model
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
        Path to GAMS file, including '.gms' extension.
        Default: ``'{model_name}.gms'`` (in the current directory).
    case : str, optional
        Run or case identifier to use in GDX file names. Default:
        ``'{scenario.model}_{scenario.name}'``, where `scenario` is the
        :class:`.Scenario` object passed to :meth:`run`.
        Formatted using `model_name` and `scenario`.
    in_file : str, optional
        Path to write GDX input file. Default: ``'{model_name}_in.gdx'``.
        Formatted using `model_name`, `scenario`, and `case`.
    out_file : str, optional
        Path to read GDX output file. Default: ``'{model_name}_out.gdx'``.
        Formatted using `model_name`, `scenario`, and `case`.
    solve_args : list of str, optional
        Arguments to be passed to GAMS, e.g. to identify the model input and
        output files. Each formatted using `model_file`, `scenario`, `case`,
        `in_file`, and `out_file`. Default:

        - ``'--in="{in_file}"'``
        - ``'--out="{out_file}"'``
    gams_args : list of str, optional
        Additional arguments passed directly to GAMS without formatting, e.g.
        to control solver options or behaviour. See the `GAMS
        Documentation <https://www.gams.com/latest/docs/UG_GamsCall.html#UG_GamsCall_ListOfCommandLineParameters>`_.
        For example:

        - ``'LogOption=4'`` prints output to stdout (not console) and the log
          file.
    check_solution : bool, optional
        If :obj:`True`, raise an exception if the GAMS solver did not reach
        optimality. (Only for MESSAGE-scheme Scenarios.)
    comment : str, optional
        Comment added to Scenario when importing the solution. If omitted, no
        comment is added.
    equ_list : list of str, optional
        Equations to be imported from the `out_file`. Default: all.
    var_list : list of str, optional
        Variables to be imported from the `out_file`. Default: all.
    """  # noqa: E501

    #: Model name.
    name = "default"

    #: Default model options.
    defaults: Mapping[str, object] = {
        "model_file": "{model_name}.gms",
        "case": "{scenario.model}_{scenario.scenario}",
        "in_file": str(Path("{temp_dir}", "{model_name}_in.gdx")),
        "out_file": str(Path("{temp_dir}", "{model_name}_out.gdx")),
        "solve_args": ['--in="{in_file}"', '--out="{out_file}"'],
        # Not formatted
        "gams_args": ["LogOption=4"],
        "check_solution": True,
        "comment": None,
        "equ_list": None,
        "var_list": None,
        "use_temp_dir": True,
    }

    def __init__(self, name=None, **model_options):
        self.model_name = name or self.name
        for arg_name, default in self.defaults.items():
            setattr(self, arg_name, model_options.get(arg_name, default))

    def run(self, scenario):
        """Execute the model."""
        backend = scenario.platform._backend

        self.scenario = scenario

        if self.use_temp_dir:
            # Create a temporary directory; automatically deleted at the end of
            # the context
            _temp_dir = TemporaryDirectory()
            self.temp_dir = _temp_dir.name

        def format(key):
            value = getattr(self, key)
            try:
                return value.format(**self.__dict__)
            except AttributeError:
                # Something like a Path; don't format it
                return value

        # Process args in order
        command = ["gams"]

        model_file = Path(format("model_file"))
        command.append('"{}"'.format(model_file))

        self.case = format("case").replace(" ", "_")
        self.in_file = Path(format("in_file"))
        self.out_file = Path(format("out_file"))

        for arg in self.solve_args:
            command.append(arg.format(**self.__dict__))

        command.extend(self.gams_args)

        if os.name == "nt":
            command = " ".join(command)

        s_arg = dict(filters=dict(scenario=scenario))
        try:
            # Write model data to file
            backend.write_file(self.in_file, ItemType.SET | ItemType.PAR, **s_arg)
        except NotImplementedError:  # pragma: no cover
            # Currently there is no such Backend
            raise NotImplementedError(
                "GAMSModel requires a Backend that can "
                "write to GDX files, e.g. JDBCBackend"
            )

        # Invoke GAMS
        cwd = self.temp_dir if self.use_temp_dir else model_file.parent
        check_call(command, shell=os.name == "nt", cwd=cwd)

        # Read model solution
        backend.read_file(
            self.out_file,
            ItemType.MODEL,
            **s_arg,
            check_solution=self.check_solution,
            comment=self.comment or "",
            equ_list=as_str_list(self.equ_list) or [],
            var_list=as_str_list(self.var_list) or [],
        )
