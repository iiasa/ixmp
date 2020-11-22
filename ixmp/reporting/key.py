from functools import lru_cache, partial
from itertools import chain, compress


class Key:
    """A hashable key for a quantity that includes its dimensionality."""

    def __init__(self, name, dims=[], tag=None):
        self._name = name
        self._dims = tuple(dims)
        self._tag = tag if isinstance(tag, str) and len(tag) else None

    @classmethod
    def from_str_or_key(cls, value, drop=[], append=[], tag=None):
        """Return a new Key from *value*.

        Parameters
        ----------
        value : str or Key
            Value to use to generate a new Key.
        drop : list of str or :obj:`True`, optional
            Existing dimensions of *value* to drop. See :meth:`drop`.
        append : list of str, optional.
            New dimensions to append to the returned Key. See :meth:`append`.
        tag : str, optional
            Tag for returned Key. If *value* has a tag, the two are joined
            using a '+' character. See :meth:`add_tag`.

        Returns
        -------
        :class:`Key`
        """
        # Determine the base Key
        if isinstance(value, cls):
            base = value
        else:
            # Parse a string
            name, *dims = value.split(":")
            _tag = dims[1] if len(dims) == 2 else None
            dims = dims[0].split("-") if len(dims) else []
            base = cls(name, dims, _tag)

        # Drop and append dimensions; add tag
        return (
            base.drop(*([drop] if drop is True else drop)).append(*append).add_tag(tag)
        )

    @classmethod
    def product(cls, new_name, *keys, tag=None):
        """Return a new Key that has the union of dimensions on *keys*.

        Dimensions are ordered by their first appearance:

        1. First, the dimensions of the first of the *keys*.
        2. Next, any additional dimensions in the second of the *keys* that
           were not already added in step 1.
        3. etc.

        Parameters
        ----------
        new_name : str
            Name for the new Key. The names of *keys* are discarded.
        """
        # Iterable of dimension names from all keys, in order, with repetitions
        dims = chain(*[k.dims for k in keys])

        # Return new key. Use dict to keep only unique *dims*, in same order
        return cls(new_name, dict.fromkeys(dims).keys()).add_tag(tag)

    def __repr__(self):
        """Representation of the Key, e.g. '<name:dim1-dim2-dim3:tag>."""
        return f"<{self}>"

    def __str__(self):
        """Representation of the Key, e.g. 'name:dim1-dim2-dim3:tag'."""
        # Use a cache so this value is only generated once; otherwise the
        # stored value is returned. This requires that the properties of the
        # key be immutable.
        @lru_cache(1)
        def _():
            return ":".join(
                [self._name, "-".join(self._dims)] + ([self._tag] if self._tag else [])
            )

        return _()

    def __hash__(self):
        """Key hashes the same as str(Key)."""
        return hash(str(self))

    def __eq__(self, other):
        """Key is equal to str(Key)."""
        return str(self) == other

    # Less-than and greater-than operations, for sorting
    def __lt__(self, other):
        if isinstance(other, (self.__class__, str)):
            return str(self) < str(other)

    def __gt__(self, other):
        if isinstance(other, (self.__class__, str)):
            return str(self) > str(other)

    @property
    def name(self):
        """Name of the quantity, :class:`str`."""
        return self._name

    @property
    def dims(self):
        """Dimensions of the quantity, :class:`tuple` of :class:`str`."""
        return self._dims

    @property
    def tag(self):
        """Quantity tag, :class:`str`."""
        return self._tag

    def drop(self, *dims):
        """Return a new Key with *dims* dropped."""
        if dims == (True,):
            new_dims = []
        else:
            new_dims = filter(lambda d: d not in dims, self.dims)
        return Key(self.name, new_dims, self.tag)

    def append(self, *dims):
        """Return a new Key with additional dimensions *dims*."""
        return Key(self.name, list(self.dims) + list(dims), self.tag)

    def add_tag(self, tag):
        """Return a new Key with *tag* appended."""
        return Key(self.name, self.dims, "+".join(filter(None, [self.tag, tag])))

    def iter_sums(self):
        """Generate (key, task) for all possible partial sums of the Key."""
        from . import computations

        for agg_dims, others in combo_partition(self.dims):
            yield (
                Key(self.name, agg_dims, self.tag),
                partial(computations.sum, dimensions=others, weights=None),
                self,
            )


def combo_partition(iterable):
    """Yield pairs of lists with all possible subsets of *iterable*."""
    # Format string for binary conversion, e.g. '04b'
    fmt = "0{}b".format(len(iterable))
    for n in range(2 ** len(iterable) - 1):
        # Two binary lists
        a, b = zip(*[(v, not v) for v in map(int, format(n, fmt))])
        yield list(compress(iterable, a)), list(compress(iterable, b))
