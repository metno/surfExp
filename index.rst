.. surfExp documentation master file, created by
   auto_sphinx.py on Sun May 18 18:19:11 2025
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

surfExp documentation
=============================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

.. include::  README.rst
.. include::  docs/example.rst

Classes
---------------------------------------------
.. autoclass:: surfexp.experiment.SettingsFromNamelistAndConfig
.. autoclass:: surfexp.experiment.SettingsFromNamelistAndConfigTactus
.. autoclass:: surfexp/suites.offline_dt_control.SurfexSuiteDefinitionDTAnalysedForcingControl
.. autoclass:: surfexp/suites.offline.SurfexSuiteDefinition
.. autoclass:: surfexp/tasks.surfex_binary_task.SurfexBinaryTask
.. autoclass:: surfexp/tasks.surfex_binary_task.OfflinePgd
.. autoclass:: surfexp/tasks.surfex_binary_task.OfflinePrep
.. autoclass:: surfexp/tasks.surfex_binary_task.OfflineForecast
.. autoclass:: surfexp/tasks.surfex_binary_task.PerturbedRun
.. autoclass:: surfexp/tasks.surfex_binary_task.Soda
.. autoclass:: surfexp/tasks.tasks.PySurfexBaseTask
.. autoclass:: surfexp/tasks.tasks.PrepareCycle
.. autoclass:: surfexp/tasks.tasks.QualityControl
.. autoclass:: surfexp/tasks.tasks.OptimalInterpolation
.. autoclass:: surfexp/tasks.tasks.CryoClim2json
.. autoclass:: surfexp/tasks.tasks.Oi2soda
.. autoclass:: surfexp/tasks.tasks.Qc2obsmon
.. autoclass:: surfexp/tasks.tasks.FirstGuess4OI
.. autoclass:: surfexp/tasks.tasks.FetchMarsObs
.. autoclass:: surfexp/tasks.tasks.HarpSQLite
.. autoclass:: surfexp/tasks.tasks.StartOfflineSfx
.. autoclass:: surfexp/tasks.compilation.CMakeBuild
.. autoclass:: surfexp/tasks.gmtedsoil.Gmted
.. autoclass:: surfexp/tasks.gmtedsoil.Soil
.. autoclass:: surfexp/tasks.fetch_mars.FetchMars
.. autoclass:: surfexp/tasks.fetch_mars.Request
.. autoclass:: surfexp/tasks.forcing.Forcing
.. autoclass:: surfexp/tasks.forcing.ModifyForcing
.. autoclass:: surfexp/tasks.forcing.Interpolate2grid

Class methods
---------------------------------------------
.. automethod:: surfexp.experiment.SettingsFromNamelistAndConfig.__init__
.. automethod:: surfexp.experiment.SettingsFromNamelistAndConfigTactus.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.SurfexBinaryTask.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.SurfexBinaryTask.get_pgdfile
.. automethod:: surfexp/tasks.surfex_binary_task.SurfexBinaryTask.execute
.. automethod:: surfexp/tasks.surfex_binary_task.OfflinePgd.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.OfflinePgd.execute
.. automethod:: surfexp/tasks.surfex_binary_task.OfflinePrep.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.OfflinePrep.execute
.. automethod:: surfexp/tasks.surfex_binary_task.OfflineForecast.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.OfflineForecast.execute
.. automethod:: surfexp/tasks.surfex_binary_task.PerturbedRun.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.PerturbedRun.execute
.. automethod:: surfexp/tasks.surfex_binary_task.Soda.__init__
.. automethod:: surfexp/tasks.surfex_binary_task.Soda.execute
.. automethod:: surfexp/tasks.tasks.PySurfexBaseTask.__init__
.. automethod:: surfexp/tasks.tasks.PySurfexBaseTask.get_exp_file_paths_file
.. automethod:: surfexp/tasks.tasks.PySurfexBaseTask.substitute
.. automethod:: surfexp/tasks.tasks.PySurfexBaseTask.get_binary
.. automethod:: surfexp/tasks.tasks.PySurfexBaseTask.get_first_guess
.. automethod:: surfexp/tasks.tasks.PySurfexBaseTask.get_forecast_start_file
.. automethod:: surfexp/tasks.tasks.PrepareCycle.__init__
.. automethod:: surfexp/tasks.tasks.PrepareCycle.run
.. automethod:: surfexp/tasks.tasks.PrepareCycle.execute
.. automethod:: surfexp/tasks.tasks.QualityControl.__init__
.. automethod:: surfexp/tasks.tasks.QualityControl.execute
.. automethod:: surfexp/tasks.tasks.OptimalInterpolation.__init__
.. automethod:: surfexp/tasks.tasks.OptimalInterpolation.execute
.. automethod:: surfexp/tasks.tasks.CryoClim2json.__init__
.. automethod:: surfexp/tasks.tasks.CryoClim2json.execute
.. automethod:: surfexp/tasks.tasks.Oi2soda.__init__
.. automethod:: surfexp/tasks.tasks.Oi2soda.execute
.. automethod:: surfexp/tasks.tasks.Qc2obsmon.__init__
.. automethod:: surfexp/tasks.tasks.Qc2obsmon.execute
.. automethod:: surfexp/tasks.tasks.FirstGuess4OI.__init__
.. automethod:: surfexp/tasks.tasks.FirstGuess4OI.execute
.. automethod:: surfexp/tasks.tasks.FirstGuess4OI.get_var_settings
.. automethod:: surfexp/tasks.tasks.FetchMarsObs.__init__
.. automethod:: surfexp/tasks.tasks.FetchMarsObs.execute
.. automethod:: surfexp/tasks.tasks.HarpSQLite.__init__
.. automethod:: surfexp/tasks.tasks.HarpSQLite.execute
.. automethod:: surfexp/tasks.tasks.StartOfflineSfx.__init__
.. automethod:: surfexp/tasks.tasks.StartOfflineSfx.execute
.. automethod:: surfexp/tasks.compilation.CMakeBuild.__init__
.. automethod:: surfexp/tasks.compilation.CMakeBuild.execute
.. automethod:: surfexp/tasks.gmtedsoil.Gmted.__init__
.. automethod:: surfexp/tasks.gmtedsoil.Gmted.get_domain_properties
.. automethod:: surfexp/tasks.gmtedsoil.Gmted.tif2bin
.. automethod:: surfexp/tasks.gmtedsoil.Gmted.execute
.. automethod:: surfexp/tasks.gmtedsoil.Soil.__init__
.. automethod:: surfexp/tasks.gmtedsoil.Soil.get_domain_properties
.. automethod:: surfexp/tasks.gmtedsoil.Soil.execute
.. automethod:: surfexp/tasks.fetch_mars.FetchMars.__init__
.. automethod:: surfexp/tasks.fetch_mars.FetchMars.execute
.. automethod:: surfexp/tasks.fetch_mars.FetchMars.fetch_mars
.. automethod:: surfexp/tasks.fetch_mars.FetchMars.split_files
.. automethod:: surfexp/tasks.fetch_mars.Request.write_request
.. automethod:: surfexp/tasks.forcing.Forcing.__init__
.. automethod:: surfexp/tasks.forcing.Forcing.execute
.. automethod:: surfexp/tasks.forcing.ModifyForcing.__init__
.. automethod:: surfexp/tasks.forcing.ModifyForcing.execute
.. automethod:: surfexp/tasks.forcing.Interpolate2grid.__init__
.. automethod:: surfexp/tasks.forcing.Interpolate2grid.execute

Methods
---------------------------------------------
.. autofunction:: surfexp.cli.pysfxexp
.. autofunction:: surfexp.experiment.check_consistency
.. autofunction:: surfexp.experiment.get_total_unique_cycle_list
.. autofunction:: surfexp.experiment.get_cycle_list
.. autofunction:: surfexp.experiment.get_fgint
.. autofunction:: surfexp/templates.cli.execute_task
.. autofunction:: surfexp/templates.stand_alone.stand_alone_main
.. autofunction:: surfexp/templates/ecflow.requeue.parse_ecflow_vars
.. autofunction:: surfexp/templates/ecflow.requeue.default_main
.. autofunction:: surfexp/templates/ecflow.default.parse_ecflow_vars
.. autofunction:: surfexp/templates/ecflow.default.default_main
.. autofunction:: surfexp/tasks.gmtedsoil.modify_ncfile
.. autofunction:: surfexp/tasks.fetch_mars._line

* :ref: `README`

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
