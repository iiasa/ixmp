Data model
**********

:mod:`ixmp` stores many types of data objects.
This page describes the ixmp *data model*, [1]_ with each kind of object in its own section.
The description here is **application independent**: it describes how :mod:`ixmp` handles data without reference to specific uses of :mod:`ixmp` to model specific real-world systems (such as :mod:`message_ix`).

.. note::
   Due to the development history of :mod:`ixmp`, some words are used with 2 or more different meanings in the data model, including ‘model’, ‘scenario’, ‘time series’, ‘meta’, ‘category’, and ‘region/node’.
   This page lists and disambiguates the multiple meanings.

.. contents::
   :local:
   :backlinks: none

.. [1] In this sense, the word “model” refers to how data is structured, and its meaning, within an :class:`ixmp.Platform`; this is distinct from a specific numeric/optimization model that can be used to solve an :class:`ixmp.Scenario` (for that, see :doc:`api-model`).

Top-level classes
=================

Platform
--------
- A Platform is identified by a string **name**.

  - This name is a reference to configuration stored in the ixmp :ref:`configuration file <configuration>`.
    Because this file is local to each user's system, the same platform name on different systems may refer to different stored data or different local or remote databases.

- A Platform contains:

  - Zero or more TimeSeries and Scenario objects.
  - :ref:`Other data associated with the Platform <data-platform>`, but not specific to any TimeSeries or Scenario.

- A Platform stores data using a back end.
  This data may be in a file, a **database** (local or remote), in memory, or elsewhere.

TimeSeries (object)
-------------------
- Each TimeSeries is uniquely identified by three attributes:

  - **model name**: an arbitrary string.
    This may refer to a model, modeling team, individual, or other data source that produced the data stored in the TimeSeries—even if that model is not implemented in :mod:`ixmp`.
  - **scenario name**: an arbitrary string.
    For the same “model name”, the scenario name can be used to distinguish multiple alternate scenarios, narratives, counterfactuals, cases, etc. created using different settings or input data, using the same model or from the same data source.
  - **version**: an integer.

- TimeSeries also have a “run ID”.
  This is a value in 1:1 correspondence with the unique (model name, scenario name, version) identifiers.
- There is no guarantee that a given (model name, scenario name, version) on one Platform refers to a TimeSeries with the same data as the same identifiers on another Platform.
- There is no correspondence between any two TimeSeries with the same model name and scenario name, but different versions.
  The two may contain entirely different data.
- For each combination of (model name, scenario name), one version may be set set as the **default version**.

.. _data-scenario:

Scenario (object)
-----------------
- Scenario is a subclass of TimeSeries and inherits its behaviour.
  This means that:

  - All of the above statements about TimeSeries objects also apply to Scenario objects.
  - All kinds of :ref:`data associated with a TimeSeries object <data-timeseries>` can also be stored within a Scenario object.
  - All statements below about TimeSeries objects also apply to Scenario objects.

- Scenarios additionally have a:

  - **scheme**: a string, that may (but does not necessarily) refer to a particular mathematical model used to solve or run the scenario, and/or corresponding list of :ref:`items <data-item>` to which the Scenario data conforms.

    For example: the scheme “MESSAGE” refers to :class:`message_ix.models.MESSAGE`, its mathematical model, and particular items.

.. _data-platform:

Data associated with a Platform
===============================

.. _codelists:

Lists of identifiers
--------------------
- A Platform stores 7 specific lists of identifiers.
- Each identifier is a string.
- Some lists have additional attributes associated with each identifier.
- Some values are pre-populated, i.e. always present on a new Platform.
- For 5 of these lists, identifiers can be added, removed, and are referenced in various ways by data in other kinds of objects.
- In some cases, they are automatically populated based on other data manipulations.

Model name
   Values for the “model name” identifier of TimeSeries objects on the Platform, as described above.

   This list is automatically extended with any model name used for a new TimeSeries, but may also contain values that are not used by any existing TimeSeries object.

Scenario name
   Values for the “scenario name” identifier of TimeSeries objects on the Platform, as described above.

   This list is automatically extended with any scenario name used for a new TimeSeries, but may also contain values that are not used by any existing TimeSeries object.

Unit
   Units of measurement.

   Values for:

   - the “unit” identifier of time-series and geodata in TimeSeries objects on the Platform.
   - the “unit” attribute of parameter data in Scenario objects on the Platform.

Region
   A geographic region or area, e.g. country, multi-country- or sub-national region, city, etc.

   Values for the “region” identifier of time-series and geodata in TimeSeries objects on the Platform.

   In addition to its ID string, each identifier has the following attributes:

   - **hierarchy**: a string, identifying 1 of multiple possible sets of of parent/child relationships.
   - **parent**: a string, optional, giving the identifier of a region which is the parent of the region.
   - **mapped_to**: a string, optional, giving the identifier of another region for which the identifier is an alias.

.. _data-timeslice:

Sub-annual time slice
   Portion of a calendar year. [3]_

   Values for the “subannual” identifier of time-series and geodata in TimeSeries objects on the Platform.

   In addition to its ID string, each identifier has the following attributes:

   - **duration**: a float number indicating the duration of the time slice, expressed in fraction of a year (dimensionless).
   - **category**: a string, identifying a set of time slices that together represent a division of one year.

   The value “Year” is automatically present, with duration ``1.0``.
   Use of this value for the “subannual” identifier indicates that the time-series or geodata **does not** have subannual resolution.

.. [3] The concept of a time slice is related to the concept represented by the index set 'time' in a :class:`message_ix.Scenario` to indicate a subannual time dimension.
   However, these are not linked automatically within :mod:`ixmp` or :mod:`message_ix` and must be defined independently.
   See :doc:`message_ix:time`.

(Metadata)
   These are the name or ID of metadata entries; see :ref:`data-meta`, below.

   This list is not directly modifiable.

(Variable)
   These are values that may appear for the “variable” identifier of time-series or geodata in TimeSeries objects on the Platform.

   This list is not directly modifiable.


.. _data-meta:

Metadata
--------
- These are a key-value store for arbitrary metadata.
- Each entry is uniquely identified by:

  - a **“meta name”** or **ID**: an arbitrary string.

- In addition each entry has:

  - a **value**: either a string, a number (floating-point, integer, or boolean), or a list of these.
  - the **target** to which it is attached or associated.
    This may be one of:

    1. A set of (model name, scenario name, version).
    2. A set of (model name, scenario name).
    3. A model name.
    4. A scenario name.

- As an artifact of some early applications, terms including “category” and “(quantitative) indicator” are variously used for the metadata identifier or metadata value.
  The term “level” is sometimes used to refer to the different kinds of targets.
- Because the name is the unique identifier, the same name cannot be used with different targets.
- The model name and/or scenario name to which an entry is associated **must** be in the :ref:`codelists` on the Platform.
  It is not required that any specific TimeSeries exist that are identified by these model name(s) and/or scenario name(s).


Documentation
-------------
- This is a second kind of key-value store for arbitrary metadata.
- Each entry is uniquely identified by:

  - A **domain**: one of “scenario”, “model”, “region”, “metadata”, “timeseries”.
  - An **identifier**. Depending on the domain, this must be a value from one of the :ref:`lists of identifiers <codelists>`:

    =========  ===
    Domain     Identifier appears in the list…
    =========  ===
    model      Model name
    scenario   Scenario name
    region     Region
    metadata   Meta, i.e. the name/ID used by :ref:`metadata <data-meta>` entries
    timseries  Variable, i.e. values for the “variable” identifier of time-series or geodata
    =========  ===

- Each entry consists of a string, e.g. containing a block of text.


.. _data-timeseries:

Data associated with a TimeSeries object
========================================

Time series data
----------------

- A TimeSeries object may contain zero or more series of time-series data.
- “series” means a 1-dimensional vector of numerical data.
- “time” means that the single dimension, called **year**, refers to a time period: either a calendar year or an identifying year in a multi-year period.

  Thus, the series consists of a mapping from years to numerical values.
- Each series is identified by:

  - **variable** name: an abitrary string.
  - **region**: a value from the “Region” list (see :ref:`codelists`).
  - **unit**: a value from the “Unit” list (see :ref:`codelists`).
  - **subannual**: a value from the “Sub-annual time slices” list (see :ref:`codelists`).
  - **meta**: a boolean value.

    .. note:: This is distinct from :ref:`data-meta`, above.


Geodata
-------

- These are identical to time-series data, except the individual values are strings instead of numbers.
- The content and meaning of the strings are user-determined.
- The name “geodata” is an artifact of the initial use-case: to store URIs or other references to geographic information systems (GIS) data, stored separately from the Platform.


.. _data-model-data:

Data associated with a Scenario object
======================================

.. _data-item:

Item
----

- A Scenario object may contain zero or more **items**.
- Each item is uniquely identified by a string **name**.
- Each item addtionally has a **type** (:class:`.ItemType`):

  - This is one of set (:attr:`.SET`), parameter (:attr:`.PAR`), variable (:attr:`.VAR`), or equation (:attr:`.EQU`).
  - This distinction is based on the data model common in algebraic modeling languages such as GAMS, Pyomo, and others.

- Because the name is the unique identifier, item names are unique *across all item types* within the same Scenario.
  For instance, it is not possible to have both a ‘set’ item and ‘parameter’ item with the name “foo”.
- The term **model data** (:attr:`.MODEL`) refers to any type of item.
- The term **model solution data** (:attr:`.SOLUTION`) refers to variable or equation data.

  Because these items are populated with data when a model is solved or run, a Scenario that contains any values for any item with either of these types is said to “contain a model solution”.

Set
---
- A *set* is either:

  1. a simple set, or
  2. an indexed set. [2]_
- A **simple**, **basic**, or **index set** is a list of strings.
- An **indexed set** has:

  - 1 or more **dimensions**.

    Each dimension is associated with a simple set, and has an optional string **name**.
    Specific values for each dimension/index set of an indexed set comprise a **key**.

  - boolean **values**.

    Each element of a index set (or each key, comprising values for 2 or more index sets) either is, or is not, a member of the indexed set.

.. [2] This distinction is also based on the GAMS data model.

Pararameter
-----------
- Parameters, variables, and equations is **indexed** by 0 or more simple sets, and thus have index sets and dimension names in the same way as described above for indexed sets.
- A parameter, variable, or equation indexed by 0 sets (0-dimensional) is a **scalar**.
- For each each key, a parameter has:

  - A single numeric **value**.
  - A **unit** attribute.

    The values of this attribute must be in the :ref:`“Unit” list <codelists>` of the Platform containing the Scenario containing the parameter.

Variable, equation
------------------
- Variables and equations have two numeric values for each key:

  - **level**: the actual value of the variable/equation.
  - **marginal**: the change in the value of the objective function of a specific optimization model for an incremental change in the variable/equation level.

- :mod:`ixmp` (as of v3.3.0) does not store unit attributes for variables and equations.
- In particular models, equations describe specific relationships between data of other types—parameters, variables, and scalars.
