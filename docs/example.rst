Examples
================================


When you set up an experiment you might first define a domain to run on.
Several pre-defined domains are defines in the surfexp/data/config/domains.
An useful visualization tool can be found in https://hirlam.github.io/nwptools/domain.html



Create and start experiments
----------------------------------

South-Norway domain
-------------------------

.. code-block:: bash

    # export DEODE_HOST="ppi_rhel8_b1"
    surfExp -o dt_offline_drammen.toml --case-name CY49DT_OFFLINE_DRAMMEN --plugin-home /home/$USER/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/domains/DRAMMEN2.toml surfexp/data/config/mods/dt_an_forcing.toml
    deode start suite --config-file dt_offline_drammen.toml


Extendended domain over Denmark
-------------------------------------

.. code-block:: bash

    surfExp edenmark.toml EDENMARK /home/$USER/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/include/domains/EDENMARK.toml surfexp/data/config/mods/dt_an_forcing.toml surfexp/data/config/mods/prep.toml
    deode start suite --config-file edenmark.toml

North-central EUROPE
---------------------------

.. code-block:: bash

    surfExp nc_europe.toml NC_EUROPE /home/$USER/projects/surfExp2 surfexp/data/config/configurations/dt.toml surfexp/data/config/include/domains/NC_EUROPE.toml surfexp/data/config/mods/dt_an_forcing.toml surfexp/data/config/mods/prep.toml
    deode start suite --config-file nc_europe.toml

DT_2_5_2500x2500
-------------------------

.. code-block:: bash

    surfExp dt_offline_dt_2_5_2500x2500.toml CY49DT_OFFLINE_dt_2_5_2500x2500 /home/$USER/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/domains/dt_2_5_2500x2500.toml surfexp/data/config/mods/dt_an_forcing.toml
    deode start suite --config-file dt_offline_dt_2_5_2500x2500.toml
