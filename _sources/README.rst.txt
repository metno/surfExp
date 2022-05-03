.. _README:

.. image:: https://coveralls.io/repos/github/metno/pysurfex-experiment/badge.svg?branch=master

https://coveralls.io/github/metno/pysurfex-experiment


This repository is a setup to create and run offline SURFEX experiments.
=========================================================================

See online documentation in https://metno.github.io/pysurfex-experiment/
The setup is dependent of pysurfex (https://metno.github.io/pysurfex) and pysurfex-scheduler (https://metno.github.io/pysurfex)

You need a python3 parser and the following dependencies are needed. Install the non-standard ones e.g. with pip or your system installation system. Requirements can be found in https://github.com/metno/pysurfex-experiment/blob/master/requirements.txt


General dependencies (from pypi)
---------------------------------

.. code-block:: bash

 pysurfex
 pysurfex-scheduler

pysurfex and pysurfex-scheduler bring extra dependencies like gridpp, titanlib and ecflow. If you install from pypi with pip these should be handled automatically.

Code unit testing and coverage
----------

Extra dependencies for testing

.. code-block:: bash

 unittest
 nose

Create coverage/test by executing:

.. code-block:: bash

 ./create_coverage.sh


Create documentation:

.. code-block:: bash

 cd docs
 # Create html documentation
 make html
 # Create latex documentation
 make latex
 # Create a pdf documentation
 make latexpdf


Usage
---------------------------------------------

Assume you have cloned experiment in pysurfex-experiment-clone-dir. Let us set some variables we can use in the examples in addition to some system settings. Adjust it to your clone, host-tag and system

.. code-block:: bash

 export PYSURFEX_EXPERIMENT_PATH="pysurfex-experiment-clone-dir"
 export HOST_TAG="my-host-tag"
 export PYSURFEX="path-to-your-pysurfex-installation"
 export OFFLINE_SOURCE_CODE="path-to-your-offline-source-code"
 export SCHEDULER_PYTHONPATH = "path-to-pysurfex-scheduler:path-to-ecflow-python-module"
 export SCHEDULER_PATH="path-to-ecflow-bin-dir"

 # Set PYTHONPATH
 export PYTHONPATH=${PYSURFEX_EXPERIMENT_PATH}:${SCHEDULER_PYTHONPATH}:$PYTHONPATH

 # Set PATH
 export PATH=${SCHEDULER_PATH}:$PATH

The reason you need to adjust the path is that the scheduler uses the ecflow start scripts to start the server.
If you have installed PYSURFEX and/or pysurfex-scheduler with pip you might try this to find the installation directories:

.. code-block:: bash

 python -c "import surfex; print(surfex.__path__[0] + '/..')"
 python -c "import scheduler; print(scheduler.__path__[0] + '/..')"


Make sure you have the host you want to run on defined in pysurfex-experiment-clone-dir/config/*/host-name-tag*. There is a file for:

- system -> sym-linked to Env_system in setup
    - Definition of experiment structure and system commands. The pythonpath for the scheduler should be defined as SCHEDULER_PYTHONPATH for each host in this file
- submit -> sym-linked to Env_submit in setup
    - Definition of submission of the job tasks, e.g to a batch system
- server -> sym-linked to  Env_server in setup
    - Definition of the scheduler running, e.g an ecflow server
- env -> sym-linked to Env in setup
    - Definition of a host specific environment used in the beginning og job files
- input_paths -> sym-linked to Env_input_paths in setup
    - Definiton of paths and structure of the experiment and input data

Now you can set up your experiment:

.. code-block:: bash

 cd
 mkdir -p sfx_home
 cd sfx_home
 mkdir -p my_exp
 cd my_exp
 ${PYSURFEX_EXPERIMENT_PATH}/bin/PySurfexExpSetup \
  -experiment ${PYSURFEX_EXPERIMENT_PATH} \
  -host ${HOST_TAG} \
  -surfex ${PYSURFEX} \
  -offline ${OFFLINE_SOURCE_CODE}
 
You now a experiment which you can modify to your needs. You can choose to run with existing binaries without specifying -offline when setting up and add the proper bin_dir to our Env_input_files file. Instead of the -experiment argument you can also specify a full existing experiment by using the -rev argument. 
 
If you run ecflow with SSL support, remember to add before starting:

.. code-block:: bash

 export ECF_SSL=1
  
This must also be added to your job tasks for example in the Env file for communication with your server fom the batch job. Finally you can start your experiment

.. code-block:: bash

 ./bin/PySurfexExp start -dtg 2022020100 -dtgend 2022020103

All batch jobs start from local json files with configuration of server, system and you configuration found on the scratch directory on the whost you run on. It means if you do local configuration changes you will need to either start/continue the run with "PySurfexExp start/prod" or you can execute PySurfexExpConfig and then sync files from HOME on HOST0 to scratch on the used hosts by executing the InitRun task in the scheduler. InitRun alone will not update your configurations, only sync files.

Examples
-----------------------

See https://metno.github.io/pysurfex-experiment/#examples


Trainings
-----------------------

`Budapest May 2022 <https://github.com/metno/pysurfex-experiment/blob/master/trainings/budapest_may_2022.rst/>`
