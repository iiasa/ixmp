from inspect import signature

try:
    from pyomo import environ as pyo, opt
    has_pyomo = True
except ImportError:
    has_pyomo = False

from ixmp.model.base import Model


COMPONENT = dict(
    par=pyo.Param,
    set=pyo.Set,
    var=pyo.Var,
    equ=pyo.Constraint,
)


def get_sets(model, names):
    return [model.component(idx_set) for idx_set in names]


class PyomoModel(Model):
    """General class for ixmp models using :mod:`pyomo`."""
    name = 'pyomo'

    items = {}
    constraints = {}
    objective = None
    _partials = {}

    def __init__(self, name=None, solver='glpk'):
        if not has_pyomo:
            raise ImportError('pyomo must be installed')

        self.opt = opt.SolverFactory(solver)

        m = pyo.AbstractModel()

        for name, info in self.items.items():
            if name == self.objective:
                # Handle the objective separately
                continue

            Component = COMPONENT[info['ix_type']]

            kwargs = {}

            if info['ix_type'] == 'equ':
                func = self.equation[name]
                params = signature(func).parameters
                idx_sets = list(params.keys())[1:]
                kwargs = dict(rule=func)
            else:
                idx_sets = info.get('idx_sets', None) or []

                # NB would like to do this, but pyomo doesn't recognize partial
                #    objects as callable
                # if info['ix_type'] != 'var':
                #     kwarg = dict(
                #         initialize=partial(self.to_pyomo, name)
                # )

            kwargs.update(self.component_kwargs.get(name, {}))

            component = Component(*get_sets(m, idx_sets), **kwargs)
            m.add_component(name, component)

        obj_func = self.equation[self.objective]
        obj = pyo.Objective(rule=obj_func, sense=pyo.minimize)
        m.add_component(self.objective, obj)

        # Store
        self.model = m

    def to_pyomo(self, name):
        info = self.items[name]
        ix_type = info['ix_type']

        if ix_type == 'par':
            item = self.scenario.par(name)

            idx_sets = info.get('idx_sets', []) or []
            if len(idx_sets):
                series = item.set_index(idx_sets)['value']
                series.index = series.index.to_flat_index()
                return series.to_dict()
            else:
                return {None: item['value']}
        elif ix_type == 'set':
            return {None: self.scenario.set(name).tolist()}

    def all_to_pyomo(self):
        return {None: dict(
            filter(
                lambda name_data: name_data[1],
                [(name, self.to_pyomo(name)) for name in self.items]
            )
        )}

    def all_from_pyomo(self, model):
        for name, info in self.items.items():
            if info['ix_type'] not in ('equ', 'var'):
                continue
            self.from_pyomo(model, name)

    def from_pyomo(self, model, name):
        component = model.component(name)
        component.display()
        try:
            data = component.get_values()
        except Exception as exc:
            print(exc)
            return

        # TODO add to Scenario; currently not possible because ixmp_source does
        # not allow setting elements of 'equ' and 'var'
        del data

    def run(self, scenario):
        self.scenario = scenario

        data = self.all_to_pyomo()

        m = self.model.create_instance(data=data)

        assert m.is_constructed()

        results = self.opt.solve(m)

        self.all_from_pyomo(m)

        delattr(self, 'scenario')
