Command-line interface
======================

:mod:`ixmp` has a **command-line** interface::

    $ ixmp --help
    Usage: ixmp [OPTIONS] COMMAND [ARGS]...

    Options:
      --url ixmp://PLATFORM/MODEL/SCENARIO[#VERSION]
                                      Scenario URL.
      --platform TEXT                 Configured platform name.
      --dbprops FILE                  Database properties file.
      --model TEXT                    Model name.
      --scenario TEXT                 Scenario name.
      --version VERSION               Scenario version.
      --help                          Show this message and exit.

    Commands:
      config         Get and set configuration keys.
      export         Export scenario data to PATH.
      import         Import time series or scenario data.
      list           List scenarios on the --platform.
      platform       Configure platforms and storage backends.
      report         Run reporting for KEY.
      show-versions  Print versions of ixmp and its dependencies.
      solve          Solve a Scenario and store results on the Platform.

The various commands allow to manipulate :ref:`configuration`, show debug and system information, invoke particular models and the :doc:`reporting` features.
The CLI is used as the basis for extended features provided by :mod:`message_ix`.

.. currentmodule:: ixmp.cli

CLI internals
-------------

.. automodule:: ixmp.cli
   :members:
