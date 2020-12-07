.. SURFEX Python API documentation master file, created by
   sphinx-quickstart on Mon Mar  2 18:25:38 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PYSURFEX documentation
=============================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

.. include::  README.rst
.. include::  docs/example.rst

Classes
---------------------------------------------
.. autoclass:: experiment.AbstractTask
.. autoclass:: experiment.SurfexBinaryTask
.. autoclass:: experiment.Pgd
.. autoclass:: experiment.Prep
.. autoclass:: experiment.Forecast
.. autoclass:: experiment.PerturbedRun
.. autoclass:: experiment.Soda
.. autoclass:: experiment.PrepareCycle
.. autoclass:: experiment.QualityControl
.. autoclass:: experiment.OptimalInterpolation
.. autoclass:: experiment.Forcing
.. autoclass:: experiment.FirstGuess
.. autoclass:: experiment.CycleFirstGuess
.. autoclass:: experiment.Oi2soda
.. autoclass:: experiment.Qc2obsmon
.. autoclass:: experiment.FirstGuess4OI
.. autoclass:: experiment.MakeOfflineBinaries
.. autoclass:: experiment.LogProgress
.. autoclass:: experiment.LogProgressPP
.. autoclass:: experiment.PrepareOiSoilInput
.. autoclass:: experiment.PrepareOiClimate
.. autoclass:: experiment.PrepareSST
.. autoclass:: experiment.PrepareLSM
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
.. autofunction:: experiment.AbstractTask.__init__
.. autofunction:: experiment.AbstractTask.run
.. autofunction:: experiment.AbstractTask.execute
.. autofunction:: experiment.AbstractTask.postfix
.. autofunction:: experiment.SurfexBinaryTask.__init__
.. autofunction:: experiment.SurfexBinaryTask.execute
.. autofunction:: experiment.Pgd.__init__
.. autofunction:: experiment.Pgd.execute
.. autofunction:: experiment.Prep.__init__
.. autofunction:: experiment.Prep.execute
.. autofunction:: experiment.Forecast.__init__
.. autofunction:: experiment.Forecast.execute
.. autofunction:: experiment.PerturbedRun.__init__
.. autofunction:: experiment.PerturbedRun.execute
.. autofunction:: experiment.Soda.__init__
.. autofunction:: experiment.Soda.execute
.. autofunction:: experiment.Soda.postfix
.. autofunction:: experiment.PrepareCycle.__init__
.. autofunction:: experiment.PrepareCycle.run
.. autofunction:: experiment.PrepareCycle.execute
.. autofunction:: experiment.QualityControl.__init__
.. autofunction:: experiment.QualityControl.execute
.. autofunction:: experiment.OptimalInterpolation.__init__
.. autofunction:: experiment.OptimalInterpolation.execute
.. autofunction:: experiment.Forcing.__init__
.. autofunction:: experiment.Forcing.execute
.. autofunction:: experiment.FirstGuess.__init__
.. autofunction:: experiment.FirstGuess.execute
.. autofunction:: experiment.CycleFirstGuess.__init__
.. autofunction:: experiment.CycleFirstGuess.execute
.. autofunction:: experiment.Oi2soda.__init__
.. autofunction:: experiment.Oi2soda.execute
.. autofunction:: experiment.Qc2obsmon.__init__
.. autofunction:: experiment.Qc2obsmon.execute
.. autofunction:: experiment.FirstGuess4OI.__init__
.. autofunction:: experiment.FirstGuess4OI.execute
.. autofunction:: experiment.FirstGuess4OI.write_file
.. autofunction:: experiment.MakeOfflineBinaries.__init__
.. autofunction:: experiment.MakeOfflineBinaries.execute
.. autofunction:: experiment.LogProgress.__init__
.. autofunction:: experiment.LogProgress.execute
.. autofunction:: experiment.LogProgressPP.__init__
.. autofunction:: experiment.LogProgressPP.execute
.. autofunction:: experiment.PrepareOiSoilInput.__init__
.. autofunction:: experiment.PrepareOiSoilInput.execute
.. autofunction:: experiment.PrepareOiClimate.__init__
.. autofunction:: experiment.PrepareOiClimate.execute
.. autofunction:: experiment.PrepareSST.__init__
.. autofunction:: experiment.PrepareSST.execute
.. autofunction:: experiment.PrepareLSM.__init__
.. autofunction:: experiment.PrepareLSM.execute
.. autofunction:: experiment.SurfexSuite.__init__
.. autofunction:: experiment.SurfexSuite.save_as_defs
.. autofunction:: experiment.System.__init__
.. autofunction:: experiment.System.get_var
.. autofunction:: experiment.SystemFromFile.__init__
.. autofunction:: experiment.Exp.checkout
.. autofunction:: experiment.Exp.setup_files
.. autofunction:: experiment.Exp.merge_testbed_submit
.. autofunction:: experiment.Exp.merge_testbed_configurations
.. autofunction:: experiment.Exp.get_file_name
.. autofunction:: experiment.Exp.get_config
.. autofunction:: experiment.Exp.get_experiment_is_locked_file
.. autofunction:: experiment.ExpFromFiles.__init__
.. autofunction:: experiment.ExpFromFiles.set_experiment_is_locked
.. autofunction:: experiment.Progress.__init__
.. autofunction:: experiment.Progress.export_to_file
.. autofunction:: experiment.Progress.get_dtgbeg
.. autofunction:: experiment.Progress.get_dtgend
.. autofunction:: experiment.Progress.increment_progress
.. autofunction:: experiment.Progress.save
.. autofunction:: experiment.ProgressFromFile.__init__
.. autofunction:: experiment.ProgressFromFile.increment_progress
.. autofunction:: experiment.SystemFilePathsFromSystem.__init__
.. autofunction:: experiment.SystemFilePathsFromSystemFile.__init__

Methods
---------------------------------------------
.. autofunction:: experiment.parse_submit_cmd_exp
.. autofunction:: experiment.submit_cmd_exp
.. autofunction:: experiment.parse_kill_cmd_exp
.. autofunction:: experiment.kill_cmd_exp
.. autofunction:: experiment.parse_status_cmd_exp
.. autofunction:: experiment.status_cmd_exp
.. autofunction:: experiment.parse_surfex_script
.. autofunction:: experiment.surfex_script
.. autofunction:: experiment.init_run


* :ref: `README`

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


