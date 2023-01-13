.. _README:

.. image:: https://coveralls.io/repos/github/metno/pysurfex-experiment/badge.svg?branch=master

https://coveralls.io/github/metno/pysurfex-experiment


This repository is a setup to create and run offline SURFEX experiments.
=========================================================================

See online documentation in https://metno.github.io/pysurfex-experiment/
The setup is dependent of pysurfex (https://metno.github.io/pysurfex)

You need a python3 parser and the following dependencies are needed. Install the non-standard ones e.g. with pip or your system installation system. Requirements can be found in https://github.com/metno/pysurfex-experiment/blob/master/requirements.txt


General dependencies (from pypi)
---------------------------------

.. code-block:: bash

 pysurfex

pysurfex bring extra dependencies like gridpp, titanlib. If you install from pypi with pip these should be handled automatically.

Installation
-------------

The recommended installation method is using poetry. First install poetry. This can be done in several ways like a system package or ftom pypi. The recommended way is:

.. code-block:: bash

 curl -sSL https://install.python-poetry.org | python3 -


When you have poetry installed make sure you have it in path. It might be installed in ~/.local/bin.
To install the script system first clone https://github.com/metno/pysurfex-experiment and install it.
To run commands interactively in the poetry environment you need to run either "poetry shell" or "poetry run [cmd]"


.. code-block:: bash

 cd
 mkdir -p projects
 cd projects

 # Clone the source code
 clone https://github.com/metno/pysurfex-experiment

 # Install the script system
 cd pysurfex-experiment
 poetry install

 # Enter the environment
 poetry shell


Usage
---------------------------------------------

Assume you have cloned experiment in pysurfex-experiment-clone-dir. Let us set some variables we can use in the examples in addition to some system settings.
Adjust it to your clone, host-tag and system. First you will set up an experiment. This will merge configuration based on your settings and split them back to configuration files.
You have the following config files in the config directories:

 * config_exp.toml
 * config_exp_surfex.toml
 * config_exp_observations.toml
 * config_exp_eps.toml
 
In addition you will get some other config files used in the tasks.

.. code-block:: bash

 export PYSURFEX_EXPERIMENT_PATH="pysurfex-experiment-clone-dir"
 export HOST_TAG="my-host-tag"
 export OFFLINE_SOURCE_CODE="path-to-your-offline-source-code"
 
 cd
 mkdir -p sfx_home
 cd sfx_home
 mkdir -p my_exp
 cd my_exp
 
 # The -offline argument is optional if you want to run with existing binaries
 PySurfexExpSetup -experiment $PYSURFEX_EXPERIMENT_PATH -host $HOST_TAG -offline $OFFLINE_SOURCE_CODE
 # This will create a file exp_dependencies.json

 # Alternative way of setting up a pre-defined SEKF configuration
 PySurfexExpSetup -experiment $PYSURFEX_EXPERIMENT_PATH -host $HOST_TAG -offline $OFFLINE_SOURCE_CODE --config sekf

 # To re-configure your config and (re-)create exp_configuration.json
 PySurfexExpConfig

 # To start you experiment
 PySurfexExp start -dtg 202301010300 -dtgend 202301010600


Following host tags are tested:

 * ECMWF-atos (ATOS at ECMWF)
 * ppi-rhel8  (RH8 PPI at met.no)
 * nebula     (nebula.nsc.liu.se)
 
 The experiment specific file exp_dependencies.json will tell you the location of the system dependent files.
 You might want to override them with local copies if needed.

Extra environment on PPI-RHEL8 needed to start experiments
---------------------------------------------------------------

.. code-block:: bash

 module use /modules/MET/rhel8/user-modules/
 module load ecflow/5.8.1
 export ECF_SSL=1


Trainings
-----------------------

`Budapest May 2022 <https://github.com/metno/pysurfex-experiment/blob/master/trainings/budapest_may_2022.rst/>`_ (Old version)
