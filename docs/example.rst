Examples
================================


When you set up an experiment you might first define a domain to run on.
Several pre-defined domains are defines in the surfexp/data/config/domains.
An useful visualization tool can be found in https://hirlam.github.io/nwptools/domain.html

Special settings

ATOS-bologna
----------------------------------

The ATOS-bologna is running on a virtual machine which does not necessarily see the troika installed in the environment. If wanted the troika command can be overridden in surfExp by adding:

.. code-block:: bash

  --troika troika

Routines needing cfunits in reading netcdf files need to modify the LD_LIBRARY_PATH. This can beachieved by adding:

.. code-block:: bash

  # TODO submission files
  [submission.task_exceptions.Forcing.ENV]
    LD_LIBRARY_PATH = "/usr/local/apps/udunits/2.2.28/lib:$LD_LIBRARY_PATH"

  [submission.task_exceptions.FirstGuess4OI.ENV]
    LD_LIBRARY_PATH = "/usr/local/apps/udunits/2.2.28/lib:$LD_LIBRARY_PATH"


MET-Norway PPI
----------------------------------

To detect the proper host on MET-Norway PPI you need to export one of the following:

.. code-block:: bash

  export DEODE_HOST="ppi_rhel8_b1"
  export DEODE_HOST="ppi_rhel8_a1"

The variables below are needed for ecflow for MET-Norway PPI

.. code-block:: bash

  module use /modules/MET/rhel8/user-modules/
  module load ecflow/5.8.1
  export ECF_SSL=1

  # Start the server if not running

  # Specify your user port in the following file
  surfexp/data/config/scheduler/ecflow_ppi_rhel8-$USER.toml


Create and start experiments
----------------------------------

South-Norway domain
-------------------------
.. code-block:: bash

    surfExp -o dt_offline_drammen.toml \
    --case-name CY49DT_OFFLINE_DRAMMEN \
    --plugin-home /home/$USER/projects/surfExp \
    --troika troika \

    surfexp/data/config/configurations/dt.toml \
    surfexp/data/config/domains/DRAMMEN.toml \
    surfExp/surfexp/data/config/mods/dev-CY49T2h_deode/dt.toml

    deode start suite --config-file dt_offline_drammen.toml


DT_2_5_2500x2500
-------------------------

.. code-block:: bash

    surfExp -o dt_offline_dt_2_5_2500x2500.toml \
    --case-name CY49DT_OFFLINE_dt_2_5_2500x2500 \
    --plugin-home /home/$USER/projects/surfExp \
    --troika troika \
    surfexp/data/config/configurations/dt.toml \
    surfexp/data/config/domains/dt_2_5_2500x2500.toml \
    surfexp/data/config/mods/dev-CY49T2h_deode/dt.toml

    deode start suite --config-file dt_offline_dt_2_5_2500x2500.toml

DT_2_5_2500x2500 Initial conditions from namelist
-------------------------

.. code-block:: bash

    surfExp -o dt_offline_dt_2_5_2500x2500.toml \
    --case-name CY49DT_OFFLINE_dt_2_5_2500x2500 \
    --plugin-home /home/$USER/projects/surfExp \
    --troika troika \
    surfexp/data/config/configurations/dt.toml \
    surfexp/data/config/domains/dt_2_5_2500x2500.toml \
    surfexp/data/config/mods/dev-CY49T2h_deode/dt.toml \
    surfexp/data/config/mods/dev-CY49T2h_deode/dt_prep_from_namelist.toml

    deode start suite --config-file dt_offline_dt_2_5_2500x2500.toml

