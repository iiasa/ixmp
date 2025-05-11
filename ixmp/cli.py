from pathlib import Path

import click

import ixmp

ScenarioClass: type[ixmp.Scenario] = ixmp.Scenario


class VersionType(click.ParamType):
    """A Click parameter type that accepts :class:`int` or 'all'."""

    name = "version"  # https://github.com/pallets/click/issues/411

    def convert(self, value, param, ctx):
        """Fail if `value` is not :class:`int` or 'all'."""
        if value == "new":
            return value
        elif isinstance(value, int):
            return value
        else:
            try:
                return int(value)
            except ValueError:
                self.fail(f"{repr(value)} must be an integer or 'new'")


@click.group()
@click.option(
    "--url", metavar="ixmp://PLATFORM/MODEL/SCENARIO[#VERSION]", help="Scenario URL."
)
@click.option("--platform", help="Configured platform name.")
@click.option(
    "--dbprops",
    type=click.Path(exists=True, dir_okay=False),
    help="Database properties file.",
)
@click.option("--model", help="Model name.")
@click.option("--scenario", help="Scenario name.")
@click.option("--version", type=VersionType(), help="Scenario version.")
@click.pass_context
def main(ctx, url, platform, dbprops, model, scenario, version):
    ctx.obj = dict()

    # Load the indicated Platform
    mp = None
    if url:
        if dbprops or platform or model or scenario or version:
            raise click.UsageError(
                "--platform --model --scenario and/or --version redundant with --url"
            )

        scen, mp = ScenarioClass.from_url(url)
        ctx.obj = dict(scen=scen, mp=mp)
        return
    elif dbprops and platform:
        raise click.UsageError("give either --platform or --dbprops")
    elif platform:
        mp = ixmp.Platform(name=platform)
    elif dbprops:
        mp = ixmp.Platform(backend="jdbc", dbprops=dbprops)

    if not mp:
        return

    ctx.obj.update(mp=mp)

    # Store the model and scenario name from arguments
    if model:
        ctx.obj["model name"] = model

    if scenario:
        ctx.obj["scenario name"] = scenario

    try:
        # Load the indicated Scenario
        if model and scenario:
            ctx.obj["scen"] = ScenarioClass(mp, model, scenario, version=version)
    except Exception as e:  # pragma: no cover
        raise click.ClickException(e.args[0])


@main.command()
@click.option("--config", help="Path to reporting configuration file.")
@click.argument("key")
@click.pass_obj
def report(context, config, key):
    """Run reporting for KEY."""
    # Import here to avoid importing reporting dependencies when running other commands
    from ixmp import Reporter

    if not context:
        raise click.UsageError(
            "give either --url, --platform or --dbprops before command report"
        )

    # Instantiate the Reporter with the Scenario loaded by main()
    r = Reporter.from_scenario(context["scen"])

    # Read the configuration file, if any
    r.configure(config)

    # Print the target
    print(r.get(key).to_series().sort_index())


@main.command("show-versions")
def show_versions_cmd():
    """Print versions of ixmp and its dependencies."""
    ixmp.show_versions()


@main.command()
@click.option(
    "--remove-solution",
    is_flag=True,
    default=False,
    help="Remove any existing model solution data.",
)
@click.pass_context
def solve(context, remove_solution):
    """Solve a Scenario and store results on the Platform.

    The scenario indicated by --url or --platform/--model/--scenario/--version is
    loaded, solved, and the solution results are saved on the Platform.

    If the scenario already has a solution, --remove-solution must be given.
    """
    if not context.obj:
        raise click.UsageError("give --url before command solve")

    print("Load scenario")

    scen = context.obj.get("scen")

    if scen is None:
        context.fail("not found")

    print("Run scenario solver")

    if remove_solution and scen.has_solution():
        scen.remove_solution()
        print("Solution removed")

    scen.solve()

    print("Solver finished")


# "config" group


@main.group("config")
def config_group():
    """Get and set configuration keys."""


@config_group.command()
@click.argument("key", metavar="KEY")
def get(key):
    """Get configuration KEY."""
    print(ixmp.config.get(key))


@config_group.command()
def show():
    """Show all configuration."""
    print(
        f"Configuration path: {ixmp.config.path}",
        ixmp.config.path.read_text(),
        sep="\n\n",
    )


@config_group.command()
@click.argument("key", metavar="KEY")
@click.argument("value", nargs=-1)
def set(key, value):
    """Set configuration KEY to VALUE."""
    try:
        ixmp.config.set(key, value[0])
    except KeyError as e:
        raise click.ClickException(f"No registered configuration key {e}")
    else:
        # Save the configuration to file
        ixmp.config.save()
        print(f"Updated {ixmp.config.path}")


@main.command()
@click.option("--max-row", type=int, help="Max row numbers in each sheet.")
@click.argument("path", type=click.Path(writable=True))
@click.argument("filter_args", metavar="[--] FILTERS", nargs=-1)
@click.pass_obj
def export(context, path, filter_args, max_row):
    """Export scenario data to PATH.

    To export only part of the parameter data, e.g. for inspection, provide FILTERS in
    the format:

        … -- dim_1=val0,val1 dim_2=val2
    """
    # NB want to use type=click.Path(..., path_type=Path), but fails on bytes
    path = Path(path)

    if not context or "scen" not in context:
        raise click.UsageError(
            "give --url, or --platform, --model, and --scenario, before export"
        )

    # Convert additional arguments into a filters dict()
    filters = dict()
    for group in filter_args:
        dim, values = group.split("=", maxsplit=1)
        filters[dim] = list(values.split(","))

    context["scen"].to_excel(path, filters=filters, max_row=max_row)


@main.group("import")
@click.pass_obj
def import_group(context):
    """Import time series or scenario data.

    DATA is the path to a file containing input data in CSV (time series only) or Excel
    format.
    """
    if not context or "scen" not in context:
        raise click.UsageError(
            "give --url, or --platform, --model, and --scenario, before command import"
        )


@import_group.command("timeseries")
@click.option("--firstyear", type=int, help="First year of data to include.")
@click.option("--lastyear", type=int, help="Final year of data to include.")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.pass_obj
def import_timeseries(context, file, firstyear, lastyear):
    """Import time series data."""
    context["scen"].read_file(Path(file), firstyear, lastyear)


@import_group.command("scenario")
@click.option(
    "--discard-solution", is_flag=True, help="Discard solution data if necessary."
)
@click.option("--add-units", is_flag=True, help="Add units to the Platform.")
@click.option("--init-items", is_flag=True, help="Initialize sets and parameters.")
@click.option("--commit-steps", is_flag=True, help="Commit after each step.")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.pass_obj
def import_scenario(
    context, file, discard_solution, add_units, init_items, commit_steps
):
    """Import scenario data."""
    scenario = context["scen"]

    if scenario.has_solution() and discard_solution:
        scenario.remove_solution()

    try:
        scenario.check_out()
    except ValueError as e:
        raise click.ClickException(e.args[0])  # Show exception message to user
    except RuntimeError as e:
        if "not yet saved" in e.args[0]:
            pass  # --version=new; no need to check out
        else:  # pragma: no cover
            raise

    scenario.read_excel(
        Path(file),
        add_units=add_units,
        init_items=init_items,
        commit_steps=commit_steps,
    )


# "platform" group


@main.group("platform")
def platform_group():
    """Configure platforms and storage backends."""


@platform_group.command()
@click.argument("name", metavar="PLATFORM")
def remove(name):
    """Remove existing PLATFORM."""
    ixmp.config.remove_platform(name)
    print(f"Removed platform config for {repr(name)}")


@platform_group.command()
@click.argument("platform_name", metavar="PLATFORM")
@click.argument("args", nargs=-1)
def add(platform_name, args):
    """Add PLATFORM, configured with ARGS.

    If PLATFORM is 'default', ARGS must be the name of another platform.

    Otherwise, the first of ARGS is the backend class (either 'jdbc' or 'ixmp4') and
    the remaining ARGS are either positional ("VALUE") or keyword ("NAME=VALUE")
    arguments.

    For the 'jdbc' backend, the remaining ARGS must be one of:

    \b
    - "oracle URL USERNAME PASSWORD", where URL is something like
      "example.com:PORT:SCHEMA". This configures a connection to an Oracle database.
    - "hsqldb PATH" where PATH is the path to the database files, including the
      directory but not the file extensions. If the files does not exist, it will be
      created.

    For the 'ixmp4' backend, the remaining ARGS must all be keyword arguments, must
    include at least "ixmp4_name=NAME". They may include:

    - "dsn=DSN". If not supplied, this is constructed automatically from ixmp4_name in
      the ixmp4 local data directory.
    - "jdbc_compat=VALUE", with VALUE like "false", "False", or "0" being treated as
      False, and all other values as True.
    """
    # Separate positional and keyword arguments
    _args, kwargs = [], {}
    for arg in args:
        arg_name, sep, value = arg.partition("=")
        if sep == "=":
            kwargs[arg_name] = value  # Keyword argument
        else:
            _args.append(arg)  # Positional argument

    ixmp.config.add_platform(platform_name, *_args, **kwargs)

    # Save the configuration to file
    ixmp.config.save()


@platform_group.command("list")
def list_platforms():
    """List configured platforms."""
    for key, info in ixmp.config.values["platform"].items():
        print(f"{key}: {info}")


@platform_group.command("copy")
@click.option("--go", is_flag=True, help="Actually manipulate files.")
@click.argument("name_source", metavar="SRC")
@click.argument("name_dest", metavar="DEST")
def copy_platform(go, name_source, name_dest):
    """Create the local JDBCBackend/HyperSQL platform DEST as a copy of SRC.

    Any existing data at DEST are overwritten. Without --go, no action occurs.
    """
    import shutil
    from copy import deepcopy

    def _check(name):
        """Retrieve platform configuration and check."""
        _, cfg = ixmp.config.get_platform_info(name)

        # Check that the source platform is supported
        info = (cfg["class"], cfg["driver"])
        if info != ("jdbc", "hsqldb"):
            msg = f"platform {name!r} has class/driver {info} != ('jdbc', 'hsqldb')"
            raise click.ClickException(msg)

        return cfg

    # Retrieve configuration for the source platform
    cfg_source = _check(name_source)

    try:
        # Retrieve configuration for the destination platform
        cfg_dest = _check(name_dest)
        add_platform = False
    except ValueError:
        # Target platform does not exist; construct its configuration
        cfg_dest = deepcopy(cfg_source)
        cfg_dest["path"] = Path(cfg_dest["path"]).parent.joinpath(name_dest)
        add_platform = True

    # Base paths for file operations
    path_source = Path(cfg_source["path"])
    dir_dest = Path(cfg_dest["path"]).parent

    msg = "" if go else "(dry run) "

    # Iterate over all files with `path_source` as a base name; skip .log and
    # .properties files and .tmp directory
    for path in filter(
        lambda p: p.suffix not in {".log", ".properties", ".tmp"},
        path_source.parent.glob(f"{path_source.stem}.*"),
    ):
        # Destination path
        path_dest = dir_dest.joinpath(name_dest).with_suffix(path.suffix)

        print(f"{msg}Copy {path} → {path_dest}")
        if not go and path_dest.exists():
            print(f"{' ' * len(msg)}(would replace existing file)")

        if go:
            shutil.copyfile(path, path_dest)

    if go and add_platform:
        # Store configuration for newly-created platform
        ixmp.config.add_platform(name_dest, "jdbc", "hsqldb", cfg_dest["path"])
        ixmp.config.save()


@main.command("list")
@click.option(
    "--match",
    metavar="EXPR",
    default=None,
    help="Regular expression for model/scenario name.",
)
@click.option(
    "--default-only", is_flag=True, help="Only scenarios with a default version."
)
@click.option("--as-url", is_flag=True, help="Display outputs as ixmp URLs.")
@click.pass_obj
def list_scenarios(context, **kwargs):
    """List scenarios on the --platform."""
    from ixmp.util import format_scenario_list

    if not context:
        raise click.UsageError(
            "give either --url, --platform or --dbprops before command list"
        )

    print(
        "\n".join(
            format_scenario_list(
                platform=context["mp"],
                model=context.get("model name", None),
                scenario=context.get("scenario name", None),
                **kwargs,
            )
        )
    )
