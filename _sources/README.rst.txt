.. _README:

.. image:: https://coveralls.io/repos/github/metno/pysurfex-experiment/badge.svg?branch=master

https://coveralls.io/github/metno/pysurfex-experiment


This repository is a setup to create and run offline SURFEX experiments.
================================================================

See online documentation in https://metno.github.io/pysurfex-experiment/
The setup is dependent of pysurfex (https://metno.github.io/pysurfex) and pysurfex-scheduler (https://metno.github.io/pysurfex)


Installation on debian based Linux system
--------------------------------------------

Install the required pacakges (some might be obsolete if the pip packages contain the needed depedencies):

.. code-block:: bash

  sudo apt-get update
  # Python tools
  sudo apt-get install python3-setuptools python3-numpy python3-nose

The following dependencies are needed. Install the non-standard ones e.g. with pip or your system installation system.


.. code-block:: bash
  sys
  argparse
  datetime
  os
  shutil
  json
  toml


General dependencies (from pypi)
---------------------------------

.. code-block:: bash

  pysurfex
  pysurfex-scheduler

pysurfex and pysurfex-scheduler bring extra dependencies like gridpp, titanlib and ecflow. If you install from pypi with pip these should be handled automatically.

For testing:

.. code-block:: bash

  unittest
  nose

Create coverage/test by executing:

.. code-block:: bash
  ./create_coverage.sh


Create documentation
---------------------------------------------

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

Assume you have cloned experiment in pysurfex-experiment-clone-dir. Let us set some variables we can use in the examples. Adjust it to your clone, host-tag and system

.. code-block:: bash
export PYSURFEX_EXPERIMENT="pysurfex-experiment-clone-dir"
export HOST_TAG="my-host-tag"
export PYSURFEX="path-to-your-pysurfex-installation"
export PYSURFEX_SCHEDULER="path-to-your-pysurfex-scheduler-installation"
export OFFLINE_SOURCE_CODE="path-to-your-offline-source-code"

If you have installed PYSURFEX and/or PYSURFEX_SCHEDULER with pip you might try this to find the installation directories:

.. code-block:: bash
python -c "import surfex; print(surfex.__path__[0] + '/..')"
python -c "import scheduler; print(scheduler.__path__[0] + '/..')"


Make sure you have the host you want to run on defined in pysurfex-experiment-clone-dir/config/*/host-name-tag*. There is a file for:
- system      Definition of experiment structure and system commands -> Env_system
- submit      Definition of submission of the job tasks, e.g to a batch system -> Env_submit
- server      Definition of the scheduler running, e.g an ecflow server -> Env_server
- env         Definition of a host specific environment used in the beginning og job files -> Env
- input_paths Definiton of paths and structure of the experiment and input data -> Env_input_paths

Now you can set up your experiment:

.. code-block:: bash
cd
mkdir -p sfx_home
cd sfx_home
mkdir -p my_exp
cd my_exp
${PYSURFEX_EXPERIMENT}/bin/PySurfexExpSetup \
  -experiment ${PYSURFEX_EXPERIMENT} \ 
  -host ${HOST_TAG} \
  -surfex ${PYSURFEX} \
  -scheduler ${PYSURFEX_SCHEDULER} \
  -offline ${OFFLINE_SOURCE_CODE}
 
You now a experiment which you can modify to your needs. You can choose to run with existing binaries without specifying -offline when setting up and add the proper bin_dir to our Env_input_files file. Instead of the -experiment argument you can also specify a full existing experiment by using the -rev argument. 
 
If you run ecflow with SSL support, remember to add before starting:

.. code-block:: bash
  export ECF_SSL=1
  
This must also be added to your job tasks for example in the Env file. Finally you can start your experiment

.. code-block:: bash
  ./bin/PySurfexExp start -dtg 2022020100 -dtgend 2022020103

Examples
-----------------------

See https://metno.github.io/pysurfex-experiment/#examples
