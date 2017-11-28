# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 13:06:09 2017

@author: huppmann
"""

import argparse
import ixmp as ix


def import_timeseries():
    args = read_args()
    mp = ix.Platform(args.dbprops)
    ix.utils.import_timeseries(mp, args.data, args.model, args.scenario,
                               args.version, args.firstyear, args.lastyear)
    mp.close_db()


# %%

def read_args():
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
    # parse cli
    args = parser.parse_args()
    return args
