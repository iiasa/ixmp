import click
import ixmp


@click.group()
@click.option('--url', metavar='ixmp://PLATFORM/MODEL/SCENARIO[#VERSION]',
              help='Scenario URL.')
@click.option('--platform', help='Configured platform name.')
@click.option('--dbprops', type=click.Path(exists=True, dir_okay=False),
              help='Database properties file.')
@click.option('--model', help='Model name.')
@click.option('--scenario', help='Scenario name.')
@click.option('--version', type=int, help='Scenario version.')
@click.pass_context
def main(ctx, url, platform, dbprops, model, scenario, version):
    # Load the indicated Platform
    if url:
        if dbprops or platform or model or scenario or version:
            raise click.BadOptionUsage('--platform --model --scenario and/or'
                                       '--version redundant with --url')

        scen, mp = ixmp.Scenario.from_url(url)
        ctx.obj = dict(scen=scen, mp=mp)
        return

    if dbprops and platform:
        raise click.BadOptionUsage('give either --platform or --dbprops')
    elif platform:
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
@click.option('--config', help='Path to reporting configuration file.')
@click.argument('key')
@click.pass_obj
def report(context, config, key):
    """Run reporting for KEY."""
    # Import here to avoid importing reporting dependencies when running
    # other commands
    from ixmp.reporting import Reporter

    # Instantiate the Reporter with the Scenario loaded by main()
    r = Reporter.from_scenario(context['scen'])

    # Read the configuration file, if any
    if config:
        r.read_config(config)

    # Print the target
    print(r.get(key))


@main.command()
@click.argument('action', type=click.Choice(['set', 'get']))
@click.argument('key', metavar='KEY')
@click.argument('value', nargs=-1)
def config(action, key, value):
    """Set/get configuration keys."""
    if action == 'get':
        if len(value):
            raise click.BadArgumentUsage("VALUE given for 'get' action")
        print(ixmp.config.get(key))
    elif action == 'set':
        ixmp.config.set(key, value[0])

        # Save the configuration to file
        ixmp.config.save()


@main.command('import')
@click.option('--firstyear', type=int, help='First year of data to include.')
@click.option('--lastyear', type=int, help='Final year of data to include.')
@click.argument('data', type=click.Path(exists=True, dir_okay=False))
@click.pass_obj
def import_command(context, firstyear, lastyear, data):
    """Import time series data.

    DATA is the path to a file containing input data in CSV or Excel format.
    """
    from ixmp.utils import import_timeseries

    import_timeseries(context['scen'], data, firstyear, lastyear)


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
