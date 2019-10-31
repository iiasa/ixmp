import argparse

import click
import ixmp


def import_timeseries():
    # construct cli
    parser = argparse.ArgumentParser()
    dbprops = 'dbprops'
    parser.add_argument('--dbprops', help=dbprops, default=None)
    data = 'data'
    parser.add_argument('--data', help=data)
    model = 'model'
    parser.add_argument('--model', help=model)
    scenario = 'scenario'
    parser.add_argument('--scenario', help=scenario)
    version = 'version'
    parser.add_argument('--version', help=version, type=str, default=None)
    firstyear = 'firstyear'
    parser.add_argument('--firstyear', help=firstyear, type=str, default=None)
    lastyear = 'lastyear'
    parser.add_argument('--lastyear', help=lastyear, type=str, default=None)
    args = parser.parse_args()

    # do the import
    mp = ixmp.Platform(args.dbprops)
    ixmp.utils.import_timeseries(mp, args.data, args.model, args.scenario,
                                 args.version, args.firstyear, args.lastyear)
    mp.close_db()


@click.group()
@click.option('--dbprops', help='Database properties file', default=None)
@click.option('--model', help='Model name', default=None)
@click.option('--scenario', help='Scenario name', default=None)
@click.option('--version', help='Scenario version', default=None)
@click.pass_context
def main(ctx, dbprops, model, scenario, version):
    """Command interface, e.g. $ ixmp COMMAND """

    # Load the indicated Platform
    if dbprops:
        mp = ixmp.Platform(dbprops)
        ctx.obj = dict(mp=mp)

        # With a Platform, load the indicated Scenario
        if model and scenario:
            scen = ixmp.Scenario(mp, model, scenario, version=version)
            ctx.obj['scen'] = scen


@main.command()
@click.option('--config', help='Path to reporting configuration file')
@click.option('--default', help='Default reporting key')
@click.pass_context
def report(ctx, config, default):
    # Import here to avoid importing reporting dependencies when running
    # other commands
    from ixmp.reporting import Reporter

    # Instantiate the Reporter with the Scenario loaded by main()
    r = Reporter.from_scenario(ctx.obj['scen'])

    # Read the configuration file, if any
    if config:
        r.read_config(config)

    # Process remaining configuration from command-line arguments
    if default:
        r.configure(default=default)

    # Print the default target
    print(r.get())


@main.command()
@click.argument('action', type=click.Choice(['set', 'get']))
@click.argument('key', metavar='KEY',
                type=click.Choice(['db_config_path', 'default_dbprops_file',
                                   'default_local_db_path']))
@click.argument('value', nargs=-1, type=click.Path())
def config(action, key, value):
    key = key.upper()

    if action == 'get':
        if len(value):
            raise click.BadArgumentUsage("VALUE given for 'get' action")
        print(ixmp.config._config.get(key))
    else:
        ixmp.config._config.set(key, value[0])

        # Save the configuration to file
        ixmp.config._config.save()
