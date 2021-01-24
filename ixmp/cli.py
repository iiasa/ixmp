from pathlib import Path
from typing import Type

import click

import ixmp

ScenarioClass: Type[ixmp.Scenario] = ixmp.Scenario


class VersionType(click.ParamType):
    """A Click parameter type that accepts :class:`int` or 'all'."""

    name = "version"  # https://github.com/pallets/click/issues/411

    def convert(self, value, param, ctx):
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
    # Load the indicated Platform
    mp = None
    if url:
        if dbprops or platform or model or scenario or version:
            raise click.UsageError(
                "--platform --model --scenario and/or " "--version redundant with --url"
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

    ctx.obj = dict(mp=mp)

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
    # Import here to avoid importing reporting dependencies when running
    # other commands
    from ixmp.reporting import Reporter

    if not context:
        raise click.UsageError(
            "give either --url, --platform or --dbprops " "before command report"
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
    help="Forces removing solution if exists.",
)
@click.pass_obj
def solve(context, remove_solution):
    """Solve a Scenario and store results on the Platform.

    The scenario indicated by --url or --platform/--model/--scenario/--version
    is loaded, solved, and the solution results are saved on the Platform.

    If the scenario already has a solution, --remove-solution must be given.
    """
    if not context:
        raise click.UsageError("give --url before command solve")

    print("Run scenario solver")
    scen = context.get("scen")
    if not scen:
        print("Scenario not found")
        return
    if remove_solution and scen.has_solution():
        scen.remove_solution()
        print("Solution removed")
    scen.solve()
    print("Solver finished")


@main.command()
@click.argument("action", type=click.Choice(["set", "get"]))
@click.argument("key", metavar="KEY")
@click.argument("value", nargs=-1)
def config(action, key, value):
    """Set/get configuration keys."""
    if action == "get":
        if len(value):
            raise click.BadArgumentUsage("VALUE given for 'get' action")
        print(ixmp.config.get(key))
    elif action == "set":
        ixmp.config.set(key, value[0])

        # Save the configuration to file
        ixmp.config.save()


@main.command()
@click.option("--max-row", type=int, help="Max row numbers in each sheet.")
@click.argument("path", type=click.Path(writable=True))
@click.argument("filter_args", metavar="[--] FILTERS", nargs=-1)
@click.pass_obj
def export(context, path, filter_args, max_row):
    """Export scenario data to PATH.

    To export only part of the parameter data, e.g. for inspection, provide
    FILTERS in the format:

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

    DATA is the path to a file containing input data in CSV (time series only)
    or Excel format.
    """
    if not context or "scen" not in context:
        raise click.UsageError(
            "give --url, or --platform, --model, and "
            "--scenario, before command import"
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


@main.command()
@click.argument("action", type=click.Choice(["add", "remove", "list"]))
@click.argument("name", required=False)
@click.argument("values", nargs=-1)
def platform(action, name, values):
    """Set/get platform configuration."""
    if action == "remove":
        assert len(values) == 0
        ixmp.config.remove_platform(name)
        print(f"Removed platform config for {repr(name)}")
    elif action == "add":
        ixmp.config.add_platform(name, *values)

        # Save the configuration to file
        ixmp.config.save()
    elif action == "list":
        for key, info in ixmp.config.values["platform"].items():
            print(key, info)


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
def list_command(context, **kwargs):
    """List scenarios on the --platform."""
    from ixmp.utils import format_scenario_list

    if not context:
        raise click.UsageError(
            "give either --url, --platform or --dbprops " "before command list"
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
