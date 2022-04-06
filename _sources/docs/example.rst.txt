
Eamples of running experiments
=======================================================

Here are some examples on how to use pysurfex-experiment

ppi-centos7 (dual-host bionic-centos7)
-------------------------------------------

First define some system paths to be used:

.. code-block:: bash

 export PYSURFEX_EXPERIMENT="/modules/bionic/user-apps/suv/pysurfex-experiment/0.0.1-dev/"
 export HOST_TAG="ppi-centos7"
 export PYSURFEX="/modules/centos7/user-apps/suv/pysurfex/0.0.1-dev/"
 export OFFLINE_SOURCE_CODE="/modules/SOURCES/centos7-SOURCES/AA_SEKF/util/offline"

  # Activate environment
 source /modules/bionic/conda/Feb2021/etc/profile.d/conda.sh
 conda activate production-04-2021

 export SCHEDULER_PYTHONPATH="/modules/bionic/user-apps/suv/pysurfex-scheduler/0.0.1-dev/:/modules/bionic/user-apps/ecflow/5.5.2-ssl/lib/python3.6/site-packages"
 export SCHEDULER_PATH="/modules/bionic/user-apps/ecflow/5.5.2-ssl/bin/"

 # Set PYTHONPATH
 export PYTHONPATH=${PYSURFEX_EXPERIMENT}:${SCHEDULER_PYTHONPATH}:$PYTHONPATH

 # Set PATH
 export PATH=${SCHEDULER_PATH}:$PATH

Default configuration on PPI

.. code-block:: bash

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir sandbox
  cd sandbox

  # Set up experiment
  ${PYSURFEX_EXPERIMENT}/bin/PySurfexExpSetup \
                  -experiment ${PYSURFEX_EXPERIMENT} \
                  -host ${HOST_TAG} \
                  -surfex ${PYSURFEX} \
                  -offline ${OFFLINE_SOURCE_CODE}

  # Sett SSL
  export ECF_SSL=1

  # Start the experiment
  ./bin/PySurfexExp start -dtg 2022013106 -dtgend 2022020206


Only snow assimilation for a domain around DRAMMEN

.. code-block:: bash

 cd
 mkdir -p sfx_home
 cd sfx_home
 mkdir DRAMMEN_SNOW_ASS
 cd DRAMMEN_SNOW_ASS

 # Set up experiment
 ${PYSURFEX_EXPERIMENT}/bin/PySurfexExpSetup \
                  -experiment ${PYSURFEX_EXPERIMENT} \
                  -host ${HOST_TAG} \
                  -surfex ${PYSURFEX} \
                  --config_file ${PYSURFEX_EXPERIMENT}/config/configurations/isba_dif_snow_ass.toml \
                  -offline ${OFFLINE_SOURCE_CODE}

 # Sett SSL
 export ECF_SSL=1

 # Start the experiment
 ./bin/PySurfexExp start -dtg 2022013106 -dtgend 2022020206
