.. SURFEX Python API documentation master file, created by
   sphinx-quickstart on Mon Mar  2 18:25:38 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PYSURFEX Experiment documentation
===================================================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

.. include::  README.rst

Classes
---------------------------------------------
.. autoclass:: experiment_tasks.AbstractTask
.. autoclass:: experiment_tasks.SurfexBinaryTask
.. autoclass:: experiment_tasks.Pgd
.. autoclass:: experiment_tasks.Prep
.. autoclass:: experiment_tasks.Forecast
.. autoclass:: experiment_tasks.PerturbedRun
.. autoclass:: experiment_tasks.Soda
.. autoclass:: experiment_tasks.PrepareCycle
.. autoclass:: experiment_tasks.QualityControl
.. autoclass:: experiment_tasks.OptimalInterpolation
.. autoclass:: experiment_tasks.Forcing
.. autoclass:: experiment_tasks.FirstGuess
.. autoclass:: experiment_tasks.CycleFirstGuess
.. autoclass:: experiment_tasks.Oi2soda
.. autoclass:: experiment_tasks.Qc2obsmon
.. autoclass:: experiment_tasks.FirstGuess4OI
.. autoclass:: experiment_tasks.MakeOfflineBinaries
.. autoclass:: experiment.SurfexSuite
.. autoclass:: experiment.System
.. autoclass:: experiment.SystemFromFile
.. autoclass:: experiment.Exp
.. autoclass:: experiment.ExpFromFiles
.. autoclass:: experiment.Progress
.. autoclass:: experiment.ProgressFromFile
.. autoclass:: experiment.SystemFilePathsFromSystem
.. autoclass:: experiment.SystemFilePathsFromSystemFile

Class methods
---------------------------------------------
.. automethod:: experiment_tasks.AbstractTask.__init__
.. automethod:: experiment_tasks.AbstractTask.run
.. automethod:: experiment_tasks.AbstractTask.execute
.. automethod:: experiment_tasks.AbstractTask.postfix
.. automethod:: experiment_tasks.SurfexBinaryTask.__init__
.. automethod:: experiment_tasks.SurfexBinaryTask.execute
.. automethod:: experiment_tasks.Pgd.__init__
.. automethod:: experiment_tasks.Pgd.execute
.. automethod:: experiment_tasks.Prep.__init__
.. automethod:: experiment_tasks.Prep.execute
.. automethod:: experiment_tasks.Forecast.__init__
.. automethod:: experiment_tasks.Forecast.execute
.. automethod:: experiment_tasks.PerturbedRun.__init__
.. automethod:: experiment_tasks.PerturbedRun.execute
.. automethod:: experiment_tasks.Soda.__init__
.. automethod:: experiment_tasks.Soda.execute
.. automethod:: experiment_tasks.Soda.postfix
.. automethod:: experiment_tasks.PrepareCycle.__init__
.. automethod:: experiment_tasks.PrepareCycle.run
.. automethod:: experiment_tasks.PrepareCycle.execute
.. automethod:: experiment_tasks.QualityControl.__init__
.. automethod:: experiment_tasks.QualityControl.execute
.. automethod:: experiment_tasks.OptimalInterpolation.__init__
.. automethod:: experiment_tasks.OptimalInterpolation.execute
.. automethod:: experiment_tasks.Forcing.__init__
.. automethod:: experiment_tasks.Forcing.execute
.. automethod:: experiment_tasks.FirstGuess.__init__
.. automethod:: experiment_tasks.FirstGuess.execute
.. automethod:: experiment_tasks.CycleFirstGuess.__init__
.. automethod:: experiment_tasks.CycleFirstGuess.execute
.. automethod:: experiment_tasks.Oi2soda.__init__
.. automethod:: experiment_tasks.Oi2soda.execute
.. automethod:: experiment_tasks.Qc2obsmon.__init__
.. automethod:: experiment_tasks.Qc2obsmon.execute
.. automethod:: experiment_tasks.FirstGuess4OI.__init__
.. automethod:: experiment_tasks.FirstGuess4OI.execute
.. automethod:: experiment_tasks.FirstGuess4OI.write_file
.. automethod:: experiment_tasks.MakeOfflineBinaries.__init__
.. automethod:: experiment_tasks.MakeOfflineBinaries.execute
.. automethod:: experiment.SurfexSuite.__init__
.. automethod:: experiment.SurfexSuite.save_as_defs
.. automethod:: experiment.System.__init__
.. automethod:: experiment.System.get_var
.. automethod:: experiment.SystemFromFile.__init__
.. automethod:: experiment.ExpFromFiles.__init__
.. automethod:: experiment.Progress.__init__
.. automethod:: experiment.Progress.save
.. automethod:: experiment.ProgressFromFile.__init__
.. automethod:: experiment.SystemFilePathsFromSystem.__init__
.. automethod:: experiment.SystemFilePathsFromSystemFile.__init__

Methods
---------------------------------------------
.. autofunction:: experiment.parse_surfex_script
.. autofunction:: experiment.surfex_script


* :ref: `README`

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


