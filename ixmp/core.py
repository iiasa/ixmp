# -*- coding: utf-8 -*-
"""
Main classes for the Python API for the ix modeling platform (ixmp)

"""

import os
from subprocess import check_call

import numpy as np
import pandas as pd

import jpype
from jpype import JPackage as java

import sys

import ixmp as ix
import ixmp.model_settings as model_settings



local_path = os.path.expanduser(os.path.join('~', '.local', 'ixmp'))

# %% default settings for column headers

iamc_idx_cols = ['model', 'scenario', 'region', 'variable', 'unit']


# %% Java Virtual Machine start-up

def start_jvm(jvmargs=None):
    if jpype.isJVMStarted():
        return

    if jvmargs is None:
        try:
            import psutil
            jvmsize = psutil.virtual_memory().available / 10**9 / 2        
            jvmargs = "-Xmx{}G".format(jvmsize)        
        except ImportError:
            jvmargs = "-Xmx4G"              

    # must add dir and jarfile to support finding ixmp.properties
    module_root = os.path.dirname(__file__)
    jarfile = os.path.join(module_root, 'ixmp.jar')
    module_lib = os.path.join(module_root, 'lib')
    module_jars = [os.path.join(module_lib, f) for f in os.listdir(module_lib)]
    sep = ';' if os.name == 'nt' else ':'
    ix_classpath = sep.join([module_root, jarfile] + module_jars)
    jvm_args = ["-Djava.class.path=" + ix_classpath, jvmargs]
    jpype.startJVM(jpype.getDefaultJVMPath(), *jvm_args)

    # define auxiliary references to Java classes
    java.ixmp = java("at.ac.iiasa.ixmp")
    java.Integer = java("java.lang").Integer
    java.Double = java("java.lang").Double
    java.LinkedList = java("java.util").LinkedList
    java.HashMap = java("java.util").HashMap
    java.LinkedHashMap = java("java.util").LinkedHashMap


# %% class ixmp.Platform


class Platform(object):
    """ The class 'Platform' is the central access point to
    the ix modeling platform (ixmp). It includes functions for managing
    and accessing TimeSeries instances (timeseries  and reference data)
    and Scenario instances (structured model input data and results).

    Parameters
    ----------
    dbprops : string
        either the name or path/name for a database.properties file
        (defaults to folder 'config', file 'default.properties')
        or the path/name for a local database (if 'dbtype' not None)
    dbtype : string
        the type of the local database (e.g., 'HSQLDB')
        if no 'dbprops' is specified, the local database is
        created/accessed at '~/.local/ixmp/localdb/default'
    jvmargs : string
        the allocated max heap space for the java virtual machine
        eg.: "-Xmx4G" (for more options see: 
        https://docs.oracle.com/javase/7/docs/technotes/tools/windows/java.html)
    """

    def __init__(self, dbprops=None, dbtype=None, jvmargs=None):
        start_jvm(jvmargs)
        self.dbtype = dbtype

        try:
            # if no dbtype is specified, launch Platform with properties file
            # or use 'config/default.properties' file
            if dbtype is None:
                dbprops = dbprops or 'default.properties'
                dbprops = dbprops if os.path.isfile(dbprops) \
                    else os.path.join(ix.default_paths.CONFIG_DIR, dbprops)
                print("launching ixmp.Platform using config file at '{}'"
                      .format(dbprops))
                self._jobj = java.ixmp.Platform("Python", dbprops)
            # if dbtype is specified, launch Platform with local database
            else:
                dbprops = dbprops or os.path.join(local_path,
                                                  'localdb', 'default')
                print("launching ixmp.Platform with local {} database at '{}'"
                      .format(dbtype, dbprops))
                self._jobj = java.ixmp.Platform("Python", dbprops, dbtype)
        except TypeError:
            msg = ("Could not launch the JVM for the ixmp.Platform."
                   "Make sure that all dependencies of ixToolbox.jar"
                   "are included in the 'ixmp/lib' folder.")
            print(msg)
            raise

    def open_db(self):
        """(re-)open the database connection of the platform instance,
        e.g., to continue working after using 'close_db()'"""
        self._jobj.openDB()

    def close_db(self):
        """close the database connection of the platform instance
        this is important when working with local database files ('HSQLDB')"""
        self._jobj.closeDB()

    def scenario_list(self, default=True, model=None, scen=None):
        """Get a list of all TimeSeries and Scenario instances
        initialized in the ixmp database instance

        Parameters
        ----------
        default : boolean, default True
            include only default model/scenario version (true) or all versions
        model : string
            the model name (optional)
        scen : string
            the scenario name (optional)
        """
        mod_scen_list = self._jobj.getScenarioList(default, model, scen)

        mod_range = range(mod_scen_list.size())
        cols = ['model', 'scenario', 'scheme', 'is_default', 'is_locked',
                'cre_user', 'cre_date', 'upd_user', 'upd_date',
                'lock_user', 'lock_date', 'annotation']

        data = {}
        for i in cols:
            data[i] = [str(mod_scen_list.get(j).get(i)) for j in mod_range]

        data['version'] = [int(str(mod_scen_list.get(j).get('version')))
                           for j in mod_range]
        cols.append("version")

        df = pd.DataFrame
        df = df.from_dict(data, orient='columns', dtype=None)
        df = df[cols]
        return df

    def TimeSeries(self, model, scen, version=None, annotation=None):
        """Initialize a new TimeSeries (timeseries or reference data)
        or get an existing TimeSeries instance from the ixmp database.

        Parameters
        ----------
        model : string
            model name
        scen : string
            scenario name
        version : string or integer
            initialize a new TimeSeries (if version='new'), or
            load a specific version from the database (if version is integer)
        annotation : string
            a short annotation/comment (when initializing a new TimeSeries)
        """
        if version == 'new':
            _jts = self._jobj.newTimeSeries(model, scen, annotation)
        elif isinstance(version, int):
            _jts = self._jobj.getTimeSeries(model, scen, version)
        else:
            _jts = self._jobj.getTimeSeries(model, scen)

        return TimeSeries(self, model, scen, _jts)

    def Scenario(self, model, scen, version=None,
                 scheme=None, annotation=None, cache=False):
        """Initialize a new ixmp.Scenario (structured input data and solution)
        or get an existing scenario from the ixmp database instance

        Parameters
        ----------
        model : string
            model name
        scen : string
            scenario name
        version : string or integer
            initialize a new scenario (if version == 'new'), or
            load a specific version from the database (if version is integer)
        scheme : string
            use an explicit scheme for initializing a new scenario
            (e.g., 'MESSAGE')
        annotation : string
            a short annotation/comment (when initializing a new scenario)
        memcache : boolean
            keep all dataframes in memory after first query (default: False)
        """
        if version == 'new':
            _jscen = self._jobj.newScenario(model, scen, scheme, annotation)
        elif isinstance(version, int):
            _jscen = self._jobj.getScenario(model, scen, version)
        else:
            _jscen = self._jobj.getScenario(model, scen)

        return Scenario(self, model, scen, _jscen, cache=cache)

    def units(self):
        """returns a list of all units initialized
        in the ixmp database instance"""
        return to_pylist(self._jobj.getUnitList())

    def add_unit(self, unit, comment='None'):
        """define a unit in the ixmp database instance

        Parameters
        ----------
        unit : string
            name of the new unit
        comment : string, default None
            annotation why this unit was added
            (timestamp and user are added automatically)
        """
        self._jobj.addUnitToDB(unit, comment)

# %% class TimeSeries


class TimeSeries(object):
    """The class 'TimeSeries' is a collection of data in timeseries format.
    It can be used for reference data, results from  models submitted
    using the IAMC template, or as parent-class of the 'Scenario' class
    to store processed model results."""

    def __init__(self, ix_mp, model, scen, _jobj):
        """initialize a new Python-class TimeSeries object
        (via the ixmp.Platform class)"""

        if not isinstance(ix_mp, Platform):
            msg = 'Do not initialize a TimeSeries directly, '
            msg += 'use ixmp.Platform.TimeSeries()!'
            raise ValueError(msg)

        self.platform = ix_mp
        self.model = model
        self.scenario = scen
        self._jobj = _jobj
        self.version = self._jobj.getVersion()

    # functions for platform management

    def check_out(self, timeseries_only=False):
        """check out from the ixmp database instance for making changes"""
        self._jobj.checkOut(timeseries_only)

    def commit(self, comment):
        """commit all changes made to the ixmp database instance"""
        self._jobj.commit(comment)
        # if version == 0, this is a new instance
        # and a new version number was assigned after the initial commit
        if self.version == 0:
            self.version = self._jobj.getVersion()

    def discard_changes(self):
        """discard all changes, reload from the ixmp database instance"""
        self._jobj.discardChanges()

    def set_as_default(self):
        """set this instance of a model/scenario as default version"""
        self._jobj.setAsDefaultVersion()

    def is_default(self):
        """ check whether this TimeSeries is set as default"""
        return bool(self._jobj.isDefault())

    def last_update(self):
        """get the timestamp of the last update/edit of this TimeSeries"""
        return self._jobj.getLastUpdateTimestamp().toString()

    def run_id(self):
        """get the run id of this TimeSeries"""
        return self._jobj.getRunId()

    def version(self):
        """get the version number of this TimeSeries"""
        return self._jobj.getVersion()

    # functions for importing and retrieving timeseries data

    def add_timeseries(self, df, meta=False):
        """add a timeseries dataframe to the TimeSeries instance

        Parameters
        ----------
        df : a Pandas dataframe either
             - in tabular form (cols: region[/node], variable, unit, year)
             - in IAMC format (cols: region[/node], variable, unit, <years>)
        meta : boolean
            indicator whether this timeseries is 'meta-data'
            (special treatment during cloning for MESSAGE-scheme scenarios)
        """
        meta = 1 if meta else 0

        if "time" in df.columns:
            raise("sub-annual time slices not supported by Python interface!")

        # rename columns to standard notation
        cols = {c: str(c).lower() for c in df.columns}
        cols.update(node='region')
        df = df.rename(columns=cols)
        if not set(iamc_idx_cols).issubset(set(df.columns)):
            missing = list(set(iamc_idx_cols) - set(df.columns))
            raise ValueError("missing required columns {}!".format(missing))

        # if in tabular format
        if ("value" in df.columns):
            df = df.sort_values(by=['region', 'variable', 'unit', 'year'])\
                .reset_index(drop=True)

            region = df.region[0]
            variable = df.variable[0]
            unit = df.unit[0]
            time = None
            jData = java.LinkedHashMap()

            for i in df.index:
                if not (region == df.region[i] and variable == df.variable[i]
                        and unit == df.unit[i]):
                    # if new 'line', pass to Java interface, start a new
                    # LinkedHashMap
                    self._jobj.addTimeseries(region, variable, time, jData,
                                             unit, meta)

                    region = df.region[i]
                    variable = df.variable[i]
                    unit = df.unit[i]
                    jData = java.LinkedHashMap()

                jData.put(java.Integer(int(df.year[i])),
                          java.Double(float(df.value[i])))
            # add the final iteration of the loop
            self._jobj.addTimeseries(region, variable, time, jData, unit, meta)

        # if in 'IAMC-style' format
        else:
            for i in df.index:
                jData = java.LinkedHashMap()

                for j in ix.utils.numcols(df):
                    jData.put(java.Integer(int(j)),
                              java.Double(float(df[j][i])))

                time = None
                self._jobj.addTimeseries(df.region[i], df.variable[i], time,
                                         jData, df.unit[i], meta)

    def timeseries(self, iamc=False, regions=None, variables=None, units=None,
                   years=None):
        """retrieve timeseries data as a pandas.DataFrame

        Parameters
        ----------
        iamc : boolean, default True
            returns a pandas.DataFrame either
            - 'IAMC-style' format (cols: region, variable unit, <years>)
            - in tabular form (cols: region, variable, unit, year)
        regions : list of strings
            filter by regions
        variables : list of strings
            filter by variables
        units : list of strings
            filter by units
        years : list of integers
            filter by years
        """

        # convert filter lists to Java objects
        regions = ix.to_jlist(regions)
        variables = ix.to_jlist(variables)
        units = ix.to_jlist(units)
        years = ix.to_jlist(years)

        # retrieve data, convert to pandas.DataFrame
        data = self._jobj.getTimeseries(regions, variables, units, None, years)
        dictionary = {}

        # if in tabular format
        ts_range = range(data.size())

        cols = ['region', 'variable', 'unit']
        for i in cols:
            dictionary[i] = [str(data.get(j).get(i)) for j in ts_range]

        dictionary['year'] = [data.get(j).get('year').intValue()
                              for j in ts_range]
        cols.append("year")

        dictionary['value'] = [data.get(j).get('value').floatValue()
                               for j in ts_range]
        cols.append("value")

        df = pd.DataFrame
        df = df.from_dict(dictionary, orient='columns', dtype=None)

        df['model'] = self.model
        df['scenario'] = self.scenario

        df = df[['model', 'scenario'] + cols]

        if iamc:
            df = df.pivot_table(index=iamc_idx_cols, columns='year')['value']
            df.reset_index(inplace=True)

        return df


# %% class Scenario

class Scenario(TimeSeries):
    """ The class 'Scenario' is a generic collection
    of all data for a model instance (sets and parameters), as well as
    the solution of a model run (levels/marginals of variables and equations).

    The class includes functions to make changes to the data,
    export all data to and import a solution from GAMS gdx,
    and save the scenario data to an ixmp database instance.
    All changes are logged for comprehensive version control.

    This class inherits all functions of the class 'TimeSeries'.
    The timeseries functions can be used to store and retrieve
    processed model outputs in the IAMC-style format."""

    _java_kwargs = {
        'set': {},
        'par': {'has_value': True},
        'var': {'has_level': True},
        'equ': {'has_level': True},
    }

    def __init__(self, ix_mp, model, scen, _jobj, cache=False):
        """initialize a new Python-class Scenario object
        (via the ixmp.Platform class)"""

        if not isinstance(ix_mp, Platform):
            msg = 'Do not initialize an Scenario directly, '
            msg += 'use ixmp.Platform.Scenario()!'
            raise ValueError(msg)

        self.platform = ix_mp
        self.model = model
        self.scenario = scen
        self._jobj = _jobj
        self.version = self._jobj.getVersion()
        self._cache = cache
        self._pycache = {}

    def item(self, ix_type, name):
        """internal function to retrieve the Java instance of an item"""
        funcs = {
            'item': self._jobj.getItem,
            'set': self._jobj.getSet,
            'par': self._jobj.getPar,
            'var': self._jobj.getVar,
            'equ': self._jobj.getEqu,
        }
        return funcs[ix_type](name)

    def element(self, ix_type, name, filters=None, cache=None):
        """internal function to retrieve a dataframe of item elements"""
        item = self.item(ix_type, name)

        # if dataframe in python cache, retrieve from there
        if name in self._pycache:
            return filtered(self._pycache[name], filters)

        # if no cache, retrieve from Java with filters
        if filters is not None and not self._cache:
            return _getElementList(item, filters, **self._java_kwargs[ix_type])

        # otherwise, retrieve from Java and keep in python cache
        df = _getElementList(item, None, **self._java_kwargs[ix_type])

        # save if using memcache
        if self._cache:
            self._pycache[name] = df

        return filtered(df, filters)

    def idx_sets(self, name):
        """return the list of index sets for an item (set, par, var, equ)

        Parameters
        ----------
        name : string
            name of the item
        """
        return to_pylist(self.item('item', name).getIdxSets())

    def idx_names(self, name):
        """return the list of index names for an item (set, par, var, equ)

        Parameters
        ----------
        name : string
            name of the item
        """
        return to_pylist(self.item('item', name).getIdxNames())

    def cat_list(self, name):
        """return a list of all categories for a set

        Parameters
        ----------
        name : string
            name of the set
        """
        return to_pylist(self._jobj.getTypeList(name))

    def add_cat(self, name, cat, keys, is_unique=False):
        """add a set element key to the respective category mapping

        Parameters
        ----------
        name : string
            name of the set
        cat : string
            name of the category
        keys : list of strings
            element keys to be added to the category mapping
        """
        self._jobj.addCatEle(name, cat, to_jlist(keys), is_unique)

    def cat(self, name, cat):
        """return a list of all set elements mapped to a category

        Parameters
        ----------
        name : string
            name of the set
        cat : string
            name of the category
        """
        return to_pylist(self._jobj.getCatEle(name, cat))

    def set_list(self):
        """return a list of sets initialized in the scenario"""
        return to_pylist(self._jobj.getSetList())

    def init_set(self, name, idx_sets=None, idx_names=None):
        """initialize a new set in the scenario

        Parameters
        ----------
        name : string
            name of the item
        idx_sets : list of strings
            index set list
        idx_names : list of strings
            index name list (optional, default to 'idx_sets')
        """
        self._jobj.initializeSet(name, _getCleanDims(idx_sets),
                                 _getCleanDims(idx_names, idx_sets))

    def set(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific set

        Parameters
        ----------
        name : string
            name of the item
        filters : dictionary
            index names mapped list of index set elements
        """
        return self.element('set', name, filters, **kwargs)

    def add_set(self, name, key, comment=None):
        """add elements to a set

        Parameters
        ----------
        name : string
            name of the set
        key : string, list/range of strings/values, dictionary, dataframe
            element(s) to be added
        comment : string, list/range of strings
            comment (optional, only used if 'key' is a string or list/range)
        """
        # delete data from the Python cache
        self.clear_cache(name)

        jSet = self.item('set', name)

        if sys.version_info[0] > 2 and isinstance(key, range):
            key = list(key)

        if (jSet.getDim() == 0) and isinstance(key, list):
            for i in range(len(key)):
                if comment and i < len(comment):
                    jSet.addElement(str(key[i]), str(comment[i]))
                else:
                    jSet.addElement(str(key[i]))
        elif isinstance(key, pd.DataFrame) or isinstance(key, dict):
            if isinstance(key, dict):
                key = pd.DataFrame.from_dict(key, orient='columns', dtype=None)
            idx_names = self.idx_names(name)
            if "comment" in list(key):
                for i in key.index:
                    jSet.addElement(to_jlist(key.ix[i], idx_names),
                                    str(key['comment'][i]))
            else:
                for i in key.index:
                    jSet.addElement(to_jlist(key.ix[i], idx_names))
        elif isinstance(key, list):
            if isinstance(key[0], list):
                for i in range(len(key)):
                    if comment and i < len(comment):
                        jSet.addElement(to_jlist(
                            key[i]), str(comment[i]))
                    else:
                        jSet.addElement(to_jlist(key[i]))
            else:
                if comment:
                    jSet.addElement(to_jlist(key), str(comment[i]))
                else:
                    jSet.addElement(to_jlist(key))
        else:
            jSet.addElement(str(key), str(comment))

    def remove_set(self, name, key=None):
        """delete a set from the scenario
        or remove an element from a set (if key is specified)

        Parameters
        ----------
        name : string
            name of the set
        key : dataframe or key list or concatenated string
            elements to be removed
        """

        # delete data from the Python cache
        self.clear_cache(name)

        if key is None:
            self._jobj.removeSet(name)
        else:
            _removeElement(self._jobj.getSet(name), key)

    def par_list(self):
        """return a list of parameters initialized in the scenario"""
        return to_pylist(self._jobj.getParList())

    def init_par(self, name, idx_sets, idx_names=None):
        """initialize a new parameter in the scenario

        Parameters
        ----------
        name : string
            name of the item
        idx_sets : list of strings
            index set list
        idx_names : list of strings
            index name list (optional, default to 'idx_sets')
        """
        self._jobj.initializePar(name, _getCleanDims(idx_sets),
                                 _getCleanDims(idx_names, idx_sets))

    def par(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific parameter

        Parameters
        ----------
        name : string
            name of the parameter
        filters : dictionary
            index names mapped list of index set elements
        """
        return self.element('par', name, filters, **kwargs)

    def add_par(self, name, key, val=None, unit=None, comment=None):
        """add elements to a parameter

        Parameters
        ----------
        name : string
            name of the parameter
        key : string, list/range of strings/values, dictionary, dataframe
            element(s) to be added
        val : values, list/range of values
            element values (only used if 'key' is a string or list/range)
        unit : string, list/range of strings
            element units (only used if 'key' is a string or list/range)
        comment : string, list/range of strings
            comment (optional, only used if 'key' is a string or list/range)
        """
        # delete data from the Python cache
        self.clear_cache(name)

        jPar = self.item('par', name)

        if sys.version_info[0] > 2 and isinstance(key, range):
            key = list(key)

        if isinstance(key, pd.DataFrame) and "key" in list(key):
            if "comment" in list(key):
                for i in key.index:
                    jPar.addElement(str(key['key'][i]),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]),
                                    str(key['comment'][i]))
            else:
                for i in key.index:
                    jPar.addElement(str(key['key'][i]),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]))

        elif isinstance(key, pd.DataFrame) or isinstance(key, dict):
            if isinstance(key, dict):
                key = pd.DataFrame.from_dict(key, orient='columns', dtype=None)
            idx_names = self.idx_names(name)
            if "comment" in list(key):
                for i in key.index:
                    jPar.addElement(to_jlist(key.ix[i], idx_names),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]),
                                    str(key['comment'][i]))
            else:
                for i in key.index:
                    jPar.addElement(to_jlist(key.ix[i], idx_names),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]))
        elif isinstance(key, list) and isinstance(key[0], list):
            unit = unit or ["???"] * len(key)
            for i in range(len(key)):
                if comment and i < len(comment):
                    jPar.addElement(to_jlist(key[i]), _jdouble(val[i]),
                                    str(unit[i]), str(comment[i]))
                else:
                    jPar.addElement(to_jlist(key[i]), _jdouble(val[i]),
                                    str(unit[i]))
        elif isinstance(key, list) and isinstance(val, list):
            unit = unit or ["???"] * len(key)
            for i in range(len(key)):
                if comment and i < len(comment):
                    jPar.addElement(str(key[i]), _jdouble(val[i]),
                                    str(unit[i]), str(comment[i]))
                else:
                    jPar.addElement(str(key[i]), _jdouble(val[i]),
                                    str(unit[i]))
        elif isinstance(key, list) and not isinstance(val, list):
            jPar.addElement(to_jlist(
                key), _jdouble(val), unit, comment)
        else:
            jPar.addElement(str(key), _jdouble(val), unit, comment)

    def init_scalar(self, name, val, unit, comment=None):
        """initialize a new scalar

        Parameters
        ----------
        name : string
            name of the scalar
        val : number
            value
        unit : string
            unit
        comment : string
            explanatory comment (optional)
        """
        jPar = self._jobj.initializePar(name, None, None)
        jPar.addElement(_jdouble(val), unit, comment)

    def scalar(self, name):
        """return a dictionary of the value and unit for a scalar

        Parameters
        ----------
        name : string
            name of the scalar
        """
        return _getElementList(self._jobj.getPar(name), None, has_value=True)

    def change_scalar(self, name, val, unit, comment=None):
        """change the value or unit of a scalar

        Parameters
        ----------
        name : string
            name of the scalar
        val : number
            value
        unit : string
            unit
        comment : string
            explanatory comment (optional)
        """
        # delete data from the Python cache
        self.clear_cache(name)
        self.item('par', name).addElement(_jdouble(val), unit, comment)

    def remove_par(self, name, key=None):
        """delete a parameter from the scenario
        or remove an element from a parameter (if key is specified)

        Parameters
        ----------
        name : string
            name of the parameter
        key : dataframe or key list or concatenated string
            elements to be removed
        """

        # delete data from the Python cache
        self.clear_cache(name)

        if key is None:
            self._jobj.removePar(name)
        else:
            _removeElement(self._jobj.getPar(name), key)

    def var_list(self):
        """return a list of variables initialized in the scenario"""
        return to_pylist(self._jobj.getVarList())

    def init_var(self, name, idx_sets=None, idx_names=None):
        """initialize a new variable in the scenario

        Parameters
        ----------
        name : string
            name of the item
        idx_sets : list of strings
            index set list
        idx_names : list of strings
            index name list (optional, default to 'idx_sets')
        """
        self._jobj.initializeVar(name, _getCleanDims(idx_sets),
                                 _getCleanDims(idx_names, idx_sets))

    def var(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific variable

        Parameters
        ----------
        name : string
            name of the variable
        filters : dictionary
            index names mapped list of index set elements
        """
        return self.element('var', name, filters, **kwargs)

    def equ_list(self):
        """return a list of equations initialized in the scenario"""
        return to_pylist(self._jobj.getEquList())

    def init_equ(self, name, idx_sets=None, idx_names=None):
        """initialize a new equation in the scenario

        Parameters
        ----------
        name : string
            name of the item
        idx_sets : list of strings
            index set list
        idx_names : list of strings
            index name list (optional, default to 'idx_sets')
        """
        self._jobj.initializeEqu(name, _getCleanDims(idx_sets),
                                 _getCleanDims(idx_names, idx_sets))

    def equ(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific equation

        Parameters
        ----------
        name : string
            name of the equation
        filters : dictionary
            index names mapped list of index set elements
        """
        return self.element('equ', name, filters, **kwargs)

    def clone(self, model=None, scen=None, annotation=None, keep_sol=True,
              first_model_year=None):
        """clone the current scenario and return the new scenario

        Parameters
        ----------
        model : string
            new model name
        scen : string
            new scenario name
        annotation : string
            explanatory comment (optional)
        keep_sol : boolean, default: True
            indicator whether to include an existing solution
            in the cloned scenario
        first_model_year: int, default None
            new first model year in cloned scenario
            ('slicing', only available for MESSAGE-scheme scenarios)
        """
        first_model_year = first_model_year or 0

        model = self.model if not model else model
        scen = self.scenario if not scen else scen
        return Scenario(self.platform, model, scen,
                        self._jobj.clone(model, scen, annotation,
                                         keep_sol, first_model_year),
                        cache=self._cache)

    def to_gdx(self, path, filename, include_var_equ=False):
        """write the scenario to GAMS gdx

        Parameters
        ----------
        path : string
            path to the folder
        filename : string
            name of the gdx file
        include_var_equ : boolean
            indicator whether to include variables/equations
            in gdx (default: False)
        """
        self._jobj.toGDX(path, filename, include_var_equ)

    def read_sol_from_gdx(self, path, filename, comment=None,
                          var_list=None, equ_list=None, check_sol=True):
        # reset Python data cache
        self.clear_cache()
        self._jobj.readSolutionFromGDX(path, filename, comment,
                                       to_jlist(var_list), to_jlist(equ_list),
                                       check_sol)

    def remove_sol(self):
        # reset Python data cache
        self.clear_cache()
        self._jobj.removeSolution()

    def solve(self, model, case=None, model_file=None,
              in_file=None, out_file=None, solve_args=None, comment=None,
              var_list=None, equ_list=None, check_sol=True):
        """solve the model (export to gdx, execute GAMS, import the solution)

        Parameters
        ----------
        model : string
            model (e.g., MESSAGE) or GAMS file name (excluding '.gms')
        case : string
            identifier of gdx file names, defaults to 'model_name_scen_name'
        model_file : string, optional
            path to GAMS file (including '.gms' extension)
        in_file : string, optional
            path to GAMS gdx input file (including '.gdx' extension)
        out_file : string, optional
            path to GAMS gdx output file (including '.gdx' extension)
        solve_args : string, optional
            arguments to be passed to GAMS (input/output file names, etc.)
        comment : string, default None
            additional comment added to changelog when importing the solution
        var_list : list of strings (optional)
            variables to be imported from the solution
        equ_list : list of strings (optional)
            equations to be imported from the solution
        check_sol : boolean, default True
            flag whether a non-optimal solution raises an exception
            (only applies to MESSAGE runs)
        """
        config = model_settings.model_config(model) \
            if model_settings.model_registered(model) \
            else model_settings.model_config('default')

        # define case name for gdx export/import, replace spaces by '_'
        case = case or '{}_{}'.format(self.model, self.scenario)
        case = case.replace(" ", "_")

        model_file = model_file or config.model_file.format(model=model)

        # define paths for writing to gdx, running GAMS, and reading a solution
        inp = in_file or config.inp.format(model=model, case=case)
        outp = out_file or config.outp.format(model=model, case=case)
        args = solve_args or [arg.format(model=model, case=case,
                                         inp=inp, outp=outp) for arg in config.args]

        ipth = os.path.dirname(inp)
        ingdx = os.path.basename(inp)
        opth = os.path.dirname(outp)
        outgdx = os.path.basename(outp)

        # write to gdx, execture GAMS, read solution from gdx
        self.to_gdx(ipth, ingdx)
        run_gams(model_file, args)
        self.read_sol_from_gdx(opth, outgdx, comment,
                               var_list, equ_list, check_sol)

    def clear_cache(self, name=None):
        """clear the Python cache of item elements

        Parameters
        ----------
        name : string
            item name (default: None, clears entire Python cache)
        """
        # if no name is given, clean the entire cache
        if name is None:
            self._pycache = {}
        # remove this element from the python data cache
        if name is not None and name in self._pycache:
            del self._pycache[name]

    def years_active(self, node, tec, yr_vtg):
        """return a list of years in which a technology of certain vintage
        at a specific node can be active

        Parameters
        ----------
        node : string
            node name
        tec : string
            name of the technology
        yr_vtg : string
            vintage year
        """
        return to_pylist(self._jobj.getTecActYrs(node, tec, str(yr_vtg)))


# %% auxiliary functions for class Scenario


def filtered(df, filters):
    if filters is None:
        return df

    mask = pd.Series(True, index=df.index)
    for k, v in filters.items():
        isin = df[k].isin(v)
        mask = mask & isin
    return df[mask]


def _getCleanDims(dims, dimsDefault=None):
    cleanDims = java.LinkedList()

    if dims is not None:
        for aDim in dims:
            cleanDims.add(aDim)
    elif dimsDefault is not None:
        for aDim in dimsDefault:
            cleanDims.add(aDim)

    return cleanDims


def _jdouble(val):
    return java.Double(float(val))


def to_pylist(jlist):
    # handling string array
    try:
        return np.array(jlist[:])
    # handling Java LinkedLists
    except Exception:
        return np.array(jlist.toArray()[:])


def to_jlist(pylist, idxName=None):
    if pylist is None:
        return None

    jList = java.LinkedList()
    if idxName is None:
        if type(pylist) is list:
            for key in pylist:
                jList.add(str(key))
        elif type(pylist) is set:
            for key in list(pylist):
                jList.add(str(key))
        else:
            jList.add(str(pylist))
    else:
        for idx in idxName:
            jList.add(str(pylist[idx]))
    return jList


def _getElementList(jItem, filters=None, has_value=False, has_level=False):

    # get list of elements, with filter HashMap if provided
    if filters is not None:
        jFilter = java.HashMap()
        for idx_name in filters.keys():
            jFilter.put(idx_name, to_jlist(filters[idx_name]))
        jList = jItem.getElements(jFilter)
    else:
        jList = jItem.getElements()

    # return a dataframe if this is a mapping or multi-dimensional parameter
    dim = jItem.getDim()
    if dim > 0:
        idx_names = np.array(jItem.getIdxNames().toArray()[:])
        idx_sets = np.array(jItem.getIdxSets().toArray()[:])

        data = {}
        for d in range(dim):
            ary = np.array(jItem.getCol(d, jList)[:])
            if idx_sets[d] == "year":
                # numpy tricks to avoid extra copy
                # _ary = ary.view('int')
                # _ary[:] = ary
                ary = ary.astype('int')
            data[idx_names[d]] = ary

        if has_value:
            data['value'] = np.array(jItem.getValues(jList)[:])
            data['unit'] = np.array(jItem.getUnits(jList)[:])

        if has_level:
            data['lvl'] = np.array(jItem.getLevels(jList)[:])
            data['mrg'] = np.array(jItem.getMarginals(jList)[:])

        df = pd.DataFrame.from_dict(data, orient='columns', dtype=None)
        return df

    else:
        #  for index sets
        if not (has_value or has_level):
            return pd.Series(jItem.getCol(0, jList)[:])

        data = {}

        # for parameters as scalars
        if has_value:
            data['value'] = jItem.getScalarValue().floatValue()
            data['unit'] = str(jItem.getScalarUnit())

        # for variables as scalars
        elif has_level:
            data['lvl'] = jItem.getScalarLevel().floatValue()
            data['mrg'] = jItem.getScalarMarginal().floatValue()

        return data


def _removeElement(jItem, key):
    """auxiliary """
    if jItem.getDim() > 0:
        if isinstance(key, list) or isinstance(key, pd.Series):
            jItem.removeElement(to_jlist(key))
        elif isinstance(key, pd.DataFrame) or isinstance(key, dict):
            if isinstance(key, dict):
                key = pd.DataFrame.from_dict(key, orient='columns', dtype=None)
            idx_names = to_pylist(jItem.getIdxNames())
            for i in key.index:
                jItem.removeElement(to_jlist(key.ix[i], idx_names))
        else:
            jItem.removeElement(str(key))

    else:
        if isinstance(key, list) or isinstance(key, pd.Series):
            jItem.removeElement(to_jlist(key))
        else:
            jItem.removeElement(str(key))


def run_gams(model_file, args, gams_args=['LogOption=4']):
    """Parameters
    ----------
    model : str
        the path to the gams file
    args : list
        arguments related to the GAMS code (input/output gdx paths, etc.)
    gams_args : list of str
        additional arguments for the CLI call to gams
        - `LogOption=4` prints output to stdout (not console) and the log file
    """
    cmd = ['gams', model_file] + args + gams_args
    cmd = cmd if os.name != 'nt' else ' '.join(cmd)
    file_path = os.path.dirname(model_file).strip('"')
    file_path = None if file_path == '' else file_path
    check_call(cmd, shell=os.name == 'nt', cwd=file_path)
