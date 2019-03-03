def aggregate(var, dims):
    return var.sum(dim=dims)


def disaggregate_shares(var, shares):
    return var * shares


def load_file(path):
    # TODO automatically parse common file formats: yaml, csv, xls(x)
    return open(path).read()


def make_dataframe(*vars):
    """Concatenate *vars* into a single pd.DataFrame."""
    # TODO also rename
    raise NotImplementedError
