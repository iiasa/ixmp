import pandas as pd
import pint


class _QuantityFactory:
    #: The current internal class used to represent reporting quantities.
    #: :meth:`as_quantity` always converts to this type.
    CLASS = 'AttrSeries'
    # CLASS = 'SparseDataArray'

    def __call__(self, data, *args, **kwargs):
        name = kwargs.pop('name', None)
        units = kwargs.pop('units', None)
        attrs = kwargs.pop('attrs', dict())

        if self.CLASS == 'AttrSeries':
            from .attrseries import AttrSeries as cls
        elif self.CLASS == 'SparseDataArray':
            from .sparsedataarray import SparseDataArray as cls

        if isinstance(data, pd.Series):
            result = cls.from_series(data)
        elif self.CLASS == 'AttrSeries':
            result = cls(data, *args, **kwargs)
        else:
            assert len(args) == len(kwargs) == 0, (args, kwargs)
            result = data._sda.convert()

        if name:
            result.name = name

        if units:
            attrs['_unit'] = pint.Unit(units)

        result.attrs.update(attrs)

        return result


Quantity = _QuantityFactory()


def assert_quantity(*args):
    """Assert that each of `args` is a Quantity object.

    Raises
    ------
    TypeError
        with a indicative message.
    """
    for i, arg in enumerate(args):
        if arg.__class__.__name__ != Quantity.CLASS:
            raise TypeError(
                f"arg #{i} ({repr(arg)}) is not Quantity; likely an incorrect "
                "key"
            )
