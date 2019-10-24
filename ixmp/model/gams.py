import os
from pathlib import Path
from subprocess import check_call


from ixmp.backend.jdbc import JDBCBackend
from ixmp.model.base import Model
from ixmp.utils import as_str_list


class GAMSModel(Model):
    """General class for ixmp models using GAMS solvers.

    GAMSModel solves a Scenario using the following steps:

    1. All Scenario data is written to a model input file in GDX format.
    2. A GAMS program is run to perform calculations.
    3. Output, or solution, data is read and stored in the Scenario.

    When created and then :meth:`run`, GAMSModel constructs file paths and
    other necessary values using format strings. The default settings are those
    in :attr:`config`; these may be overridden by the keyword arguments to
    the constructor.

    Other parameters
    ----------------
    name : str
        Override the :attr:`name` attribute.
    model_file : str
        Path to GAMS file, including '.gms' extension.
    case : str
        Run or case identifier to use in GDX file names. Default:
        '{scenario.model}_{scenario.name}', where *scenario* is the Scenario
        object passed to :meth:`run`. Formatted using *model_file* and
        *scenario*.
    in_file : str
        Path to write GDX input file. Formatted using *model_file*,
        *scenario*, and *case*.
    out_file : str
        Path to read GDX output file. Formatted using *model_file*,
        *scenario*, and *case*.
    solve_args : list of str
        Arguments to be passed to GAMS. Each formatted using *model_file*,
        *scenario*, *case*, *in_file*, and *out_file*.
    gams_args : list of str
        Additional arguments passed directly to GAMS. See, e.g.,
        https://www.gams.com/latest/docs/UG_GamsCall.html#UG_GamsCall_ListOfCommandLineParameters

        - “LogOption=4” prints output to stdout (not console) and the log
          file.
    check_solution : bool
        If True, raise an exception if the GAMS solver did not reach
        optimality. (Only for MESSAGE-scheme Scenarios.)
    comment : str
        Comment added to Scenario when importing the solution.
    equ_list : list of str
        Equations to be imported from the *out_file*.
    var_list : list of str
        Variables to be imported from the *out_file*.
    """

    #: Model name.
    name = 'default'

    #: Default model options.
    defaults = {
        'model_file': '{model_name}.gms',
        'case': "{scenario.model}_{scenario.scenario}",
        'in_file': '{model_name}_in.gdx',
        'out_file': '{model_name}_out.gdx',
        'solve_args': ['--in="{in_file}"', '--out="{out_file}"'],
        # Not formatted
        'gams_args': ['LogOption=4'],
        'check_solution': True,
        'comment': None,
        'equ_list': None,
        'var_list': None,
    }

    def __init__(self, name=None, **model_options):
        self.model_name = name or self.name
        for arg_name, default in self.defaults.items():
            setattr(self, arg_name, model_options.get(arg_name, default))

    def run(self, scenario):
        """Execute the model."""
        if not isinstance(scenario.platform._backend, JDBCBackend):
            raise ValueError('GAMSModel can only solve Scenarios with '
                             'JDBCBackend')

        self.scenario = scenario

        def format(key):
            value = getattr(self, key)
            try:
                return value.format(**self.__dict__)
            except AttributeError:
                # Something like a Path; don't format it
                return value

        # Process args in order
        command = ['gams']

        model_file = Path(format('model_file')).resolve()
        command.append('"{}"'.format(model_file))

        self.case = format('case').replace(' ', '_')
        self.in_file = Path(format('in_file')).resolve()
        self.out_file = Path(format('out_file')).resolve()

        for arg in self.solve_args:
            command.append(arg.format(**self.__dict__))

        command.extend(self.gams_args)

        if os.name == 'nt':
            command = ' '.join(command)

        # Write model data to file
        scenario._backend('write_gdx', self.in_file)

        # Invoke GAMS
        check_call(command, shell=os.name == 'nt', cwd=model_file.parent)

        # Reset Python data cache
        scenario.clear_cache()

        # Read model solution
        scenario._backend('read_gdx', self.out_file,
                          self.check_solution,
                          self.comment,
                          as_str_list(self.equ_list),
                          as_str_list(self.var_list),
                          )
