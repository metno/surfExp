
Running an experiment in EcFlow
=======================================================

First you must install pysurfex-scheduler and pysurfex-experiment and make sure you have it in PYTHONPATH and the bin
directory in your path.

Default setup on PPI

.. code-block:: bash

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir sandbox
  cd sandbox

  # Set up experiemnt
  PySurfexExpSetup -rev /modules/bionic/user-apps/suv/pysurfex-experiment/0.0.1-dev/ \
                   -host ppi-centos7 \
                   -surfex /modules/centos7/user-apps/suv/pysurfex/0.0.1-dev/ \
                   -offline /modules/SOURCES/centos7-SOURCES/AA_SEKF/util/offline

  ./bin/PySurfexExp start -dtg 2022013106 -dtgend 2022020206


Only snow assimilation on PPI for a domain around DRAMMEN

.. code-block:: bash

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir DRAMMEN_SNOW_ASS
  cd DRAMMEN_SNOW_ASS

  # Set up experiemnt
  PySurfexExpSetup -rev /modules/bionic/user-apps/suv/pysurfex-experiment/0.0.1-dev/ \
                   -host ppi-centos7 \
                   -surfex /modules/centos7/user-apps/suv/pysurfex/0.0.1-dev/ \
                   --config_file /modules/bionic/user-apps/suv/pysurfex-experiment/0.0.1-dev/config/configurations/isba_dif_snow_ass.toml \
                   -offline /modules/SOURCES/centos7-SOURCES/AA_SEKF/util/offline/conf/system.ppi_centos7

  ./bin/PySurfexExp start -dtg 2022013106 -dtgend 2022020206
