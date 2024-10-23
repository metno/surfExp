.. SURFEX Python API documentation master file, created by
   sphinx-quickstart on Mon Mar  2 18:25:38 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

surfExp documentation
===================================================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

.. include::  README.rst

Classes
-------------------------------------------------------------------------------
.. autoclass:: surfexp.tasks.tasks.PySurfexBaseTask
.. autoclass:: surfexp.tasks.surfex_binary_task.SurfexBinaryTask
.. autoclass:: surfexp.tasks.surfex_binary_task.OfflinePgd
.. autoclass:: surfexp.tasks.surfex_binary_task.OfflinePrep
.. autoclass:: surfexp.tasks.surfex_binary_task.OfflineForecast
.. autoclass:: surfexp.tasks.surfex_binary_task.PerturbedRun
.. autoclass:: surfexp.tasks.surfex_binary_task.Soda
.. autoclass:: surfexp.tasks.tasks.PrepareCycle
.. autoclass:: surfexp.tasks.tasks.QualityControl
.. autoclass:: surfexp.tasks.tasks.OptimalInterpolation
.. autoclass:: surfexp.tasks.forcing.Forcing
.. autoclass:: surfexp.tasks.tasks.FirstGuess
.. autoclass:: surfexp.tasks.tasks.CycleFirstGuess
.. autoclass:: surfexp.tasks.tasks.Oi2soda
.. autoclass:: surfexp.tasks.tasks.Qc2obsmon
.. autoclass:: surfexp.tasks.tasks.FirstGuess4OI
.. autoclass:: surfexp.tasks.compilation.MakeOfflineBinaries
.. autoclass:: surfexp.tasks.compilation.SyncSourceCode
.. autoclass:: surfexp.tasks.compilation.ConfigureOfflineBinaries
.. autoclass:: surfexp.tasks.compilation.CMakeBuild


Class methods
---------------------------------------------
.. automethod:: surfexp.tasks.tasks.PySurfexBaseTask.__init__
.. automethod:: surfexp.tasks.tasks.PySurfexBaseTask.run
.. automethod:: surfexp.tasks.tasks.PySurfexBaseTask.execute
.. automethod:: surfexp.tasks.surfex_binary_task.SurfexBinaryTask.__init__
.. automethod:: surfexp.tasks.surfex_binary_task.SurfexBinaryTask.run
.. automethod:: surfexp.tasks.surfex_binary_task.SurfexBinaryTask.execute
.. automethod:: surfexp.tasks.surfex_binary_task.OfflinePgd.__init__
.. automethod:: surfexp.tasks.surfex_binary_task.OfflinePgd.run
.. automethod:: surfexp.tasks.surfex_binary_task.OfflinePgd.execute
.. automethod:: surfexp.tasks.surfex_binary_task.OfflinePrep.__init__
.. automethod:: surfexp.tasks.surfex_binary_task.OfflinePrep.run
.. automethod:: surfexp.tasks.surfex_binary_task.OfflinePrep.execute 
.. automethod:: surfexp.tasks.surfex_binary_task.OfflineForecast.__init__
.. automethod:: surfexp.tasks.surfex_binary_task.OfflineForecast.run
.. automethod:: surfexp.tasks.surfex_binary_task.OfflineForecast.execute
.. automethod:: surfexp.tasks.surfex_binary_task.PerturbedRun.__init__
.. automethod:: surfexp.tasks.surfex_binary_task.PerturbedRun.run
.. automethod:: surfexp.tasks.surfex_binary_task.PerturbedRun.execute
.. automethod:: surfexp.tasks.surfex_binary_task.Soda.__init__
.. automethod:: surfexp.tasks.surfex_binary_task.Soda.run
.. automethod:: surfexp.tasks.surfex_binary_task.Soda.execute
.. automethod:: surfexp.tasks.tasks.PrepareCycle.__init__
.. automethod:: surfexp.tasks.tasks.PrepareCycle.run
.. automethod:: surfexp.tasks.tasks.PrepareCycle.execute
.. automethod:: surfexp.tasks.tasks.QualityControl.__init__
.. automethod:: surfexp.tasks.tasks.QualityControl.run
.. automethod:: surfexp.tasks.tasks.QualityControl.execute
.. automethod:: surfexp.tasks.tasks.OptimalInterpolation.__init__
.. automethod:: surfexp.tasks.tasks.OptimalInterpolation.run
.. automethod:: surfexp.tasks.tasks.OptimalInterpolation.execute
.. automethod:: surfexp.tasks.forcing.Forcing.__init__
.. automethod:: surfexp.tasks.forcing.Forcing.run
.. automethod:: surfexp.tasks.forcing.Forcing.execute
.. automethod:: surfexp.tasks.tasks.FirstGuess.__init__
.. automethod:: surfexp.tasks.tasks.FirstGuess.run
.. automethod:: surfexp.tasks.tasks.FirstGuess.execute
.. automethod:: surfexp.tasks.tasks.CycleFirstGuess.__init__
.. automethod:: surfexp.tasks.tasks.CycleFirstGuess.run
.. automethod:: surfexp.tasks.tasks.CycleFirstGuess.execute
.. automethod:: surfexp.tasks.tasks.Oi2soda.__init__
.. automethod:: surfexp.tasks.tasks.Oi2soda.run
.. automethod:: surfexp.tasks.tasks.Oi2soda.execute
.. automethod:: surfexp.tasks.tasks.Qc2obsmon.__init__
.. automethod:: surfexp.tasks.tasks.Qc2obsmon.run
.. automethod:: surfexp.tasks.tasks.Qc2obsmon.execute
.. automethod:: surfexp.tasks.tasks.FirstGuess4OI.__init__
.. automethod:: surfexp.tasks.tasks.FirstGuess4OI.run
.. automethod:: surfexp.tasks.tasks.FirstGuess4OI.execute
.. automethod:: surfexp.tasks.compilation.MakeOfflineBinaries.__init__
.. automethod:: surfexp.tasks.compilation.MakeOfflineBinaries.run
.. automethod:: surfexp.tasks.compilation.MakeOfflineBinaries.execute
.. automethod:: surfexp.tasks.compilation.SyncSourceCode.__init__
.. automethod:: surfexp.tasks.compilation.SyncSourceCode.run
.. automethod:: surfexp.tasks.compilation.SyncSourceCode.execute
.. automethod:: surfexp.tasks.compilation.ConfigureOfflineBinaries.__init__
.. automethod:: surfexp.tasks.compilation.ConfigureOfflineBinaries.run
.. automethod:: surfexp.tasks.compilation.ConfigureOfflineBinaries.execute
.. automethod:: surfexp.tasks.compilation.CMakeBuild.__init__
.. automethod:: surfexp.tasks.compilation.CMakeBuild.run
.. automethod:: surfexp.tasks.compilation.CMakeBuild.execute
   

Methods
---------------------------------------------
.. autofunction:: surfexp.templates.cli.execute_task
.. autofunction:: surfexp.cli.pysfxexp
.. autofunction:: surfexp.experiment.get_nnco
.. autofunction:: surfexp.experiment.get_total_unique_cycle_list
.. autofunction:: surfexp.experiment.get_cycle_list
.. autofunction:: surfexp.experiment.get_setting
.. autofunction:: surfexp.experiment.setting_is
.. autofunction:: surfexp.experiment.get_fgint


* :ref: `README`

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


