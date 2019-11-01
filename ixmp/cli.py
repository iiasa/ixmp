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
@click.option('--platform', help='Configured platform name', default=None)
@click.option('--dbprops', help='Database properties file', default=None)
@click.option('--model', help='Model name', default=None)
@click.option('--scenario', help='Scenario name', default=None)
@click.option('--version', help='Scenario version', default=None)
@click.pass_context
def main(ctx, platform, dbprops, model, scenario, version):
    """Command interface, e.g. $ ixmp COMMAND """

    # Load the indicated Platform
    if dbprops and platform:
        raise click.BadOptionUsage('give either --platform or --dbprops')

    if platform:
        mp = ixmp.Platform(name=platform)
    elif dbprops:
        mp = ixmp.Platform(backend='jdbc', dbprops=dbprops)

    try:
        ctx.obj = dict(mp=mp)
    except NameError:
        return

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
    """Set/get configuration keys."""
    key = key.upper()

    if action == 'get':
        if len(value):
            raise click.BadArgumentUsage("VALUE given for 'get' action")
        print(ixmp.config.get(key))
    elif action == 'set':
        ixmp.config.set(key, value[0])

        # Save the configuration to file
        ixmp.config.save()


@main.command()
@click.argument('action', type=click.Choice(['add', 'remove', 'list']))
@click.argument('name', required=False)
@click.argument('values', nargs=-1)
def platform(action, name, values):
    """Set/get platform configuration."""
    if action == 'remove':
        assert len(values) == 0
        ixmp.config.remove_platform(name)
        print('Removed platform config for {!r}'.format(name))
    elif action == 'add':
        ixmp.config.add_platform(name, *values)

        # Save the configuration to file
        ixmp.config.save()
    elif action == 'list':
        for key, info in ixmp.config.values['platform'].items():
            print(key, info)
