import argparse
import sys

import ixmp


def config():
    # construct cli
    parser = argparse.ArgumentParser()
    db_config_path = ('Set default directory for database connection and '
                      'configuration files.')
    parser.add_argument('--db_config_path', help=db_config_path, default=None)
    default_dbprops_file = ('Set default properties file for database '
                            ' connection.')
    parser.add_argument('--default_dbprops_file',
                        help=default_dbprops_file, default=None)
    args = parser.parse_args()

    # Store the user-supplied configuration values
    ixmp.config._config.set('DB_CONFIG_PATH', args.db_config_path)
    ixmp.config._config.set('DEFAULT_DBPROPS_FILE', args.default_dbprops_file)

    # Save the configuration to file
    ixmp.config._config.save()


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


def main():
    """Command interface, e.g. $ ixmp COMMAND """
    # Only supported command
    assert sys.argv[1] == 'report'
    del sys.argv[1]
    report()


def report():
    from ixmp.reporting import Reporter, parse_reporting_args

    # Parse reporting-related arguments
    r_config, remaining = parse_reporting_args(sys.argv)
    r_config = vars(r_config)

    # Parse non-reporting arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbprops', help='database properties file')
    parser.add_argument('--model', help='model name')
    parser.add_argument('--scenario', help='scenario name')
    parser.add_argument('--version', help='version', type=str, default=None)
    args = parser.parse_args(remaining[1:])

    # Load the indicated scenario
    mp = ixmp.Platform(args.dbprops)
    scen = ixmp.Scenario(mp, args.model, args.scenario, version=args.version)

    # Instantiate the Reporter with the given scenario
    r = Reporter.from_scenario(scen)

    # Read the configuration file, if any
    if 'config' in r_config:
        r.read_config(r_config.pop('config'))

    # Process remaining configuration from command-line arguments
    r.configure(**r_config)

    # Print the default target
    print(r.get())
