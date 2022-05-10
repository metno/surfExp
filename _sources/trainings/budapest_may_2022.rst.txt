#########################################
 ACCORD training Budapest May 9-13 20222
#########################################

Excercises are prepared for the ACCORD training in Budapest. More information can be found on the internal website https://opensource.umr-cnrm.fr/projects/accord/wiki/Spring_Surface_Working_Week_2022

Sample data can be downloaded from ECMWF ecgate:
 - /hpc/perm/ms/no/sbu/training/budapest_2022_pysurfex_training_data.tgz
 - /hpc/perm/ms/no/sbu/training/budapest_2022.tgz
 
Source code:
 - /hpc/perm/ms/no/sbu/training/AA_preop2_surfex_v1.tgz
 - /hpc/perm/ms/no/sbu/training/auxlib.tgz

For installation of pysurfex, scheduler and experiment I reccomend to clone the repos to your system. Then install the extra dependencies from pip:
https://github.com/metno/pysurfex/blob/master/requirements.txt
https://github.com/metno/pysurfex-experiment/blob/master/requirements.txt
https://github.com/metno/pysurfex-scheduler/blob/master/requirements.txt

You will need python3 and I reccomend to install either with "pip3 install [package] --user" or python3 -m pip install [package] --user. This does not require special permissions and will install in ~/.local. You can of course also install it system-wide but not everyone can do this.

.. code-block:: bash

  # extra-dependencies is the path to ~/.local or wherever you put your extra needed dependencies installed above
  export PYTHONPATH=[path-to-pysurfex-clone]:[path-to-pysurfex-experimet-clone]:[path-to-pysurfex-experiment-clone]:[extra-dependencies]:$PYTHONPATH
  export PATH=[path-to-pysurfex-clone]/bin:[path-to-pysurfex-experiment-clone]/bin:$PATH

This can for example be combined into a environment module if you for example do this on a hpc with a software module system. By running module use [path-to-module-files] you can always run user defined modules.

The first and second parts in the exercises will be applications purely based on the pySurfex repository. The third part will be applications with pysurfex-experiment. Here instructions will be made on how to set it up yourself or use (pseudo) pre-configured setup on ecgate-cca at ECMWF.


================================================================================
Part 1: Run offline binaries and create a first guess
================================================================================

The first part demonstrates examples on how to make forcing and run offline SURFEX.
The second part will focus on observation pre-processing, horizontal analysis and surface assimilation.

Assumptions:

- Assume pysurfex is installed/cloned and in your path (https://github.com/metno/pysurfex)
- Assume pysurfex installation/clone directory is "path-to-pysurfex"
- Assume pysurfex-experiment installation/clone directory is "path-to-pysurfex-experiment"
- Assume that you have your surfex binaries in PATH and that they are called PGD, PREP, OFFLINE and SODA
- Assume that you have system paths defined in a file called system.json

  - LAKE_LTA_NEW.nc (flake_dir) needed for PREP
- nobackup/trainingData/config_exp.toml is consistent with AA preop2 and can be found in sample data. It is assumed to be relative to where you run.
- Examples will use a test domain called Drammen close to Oslo in Norway. Domain is found in [path-to-pysurfex]/examples/domains/drammen.json


E1.1: Create offline forcing using the MET-Nordic analysis from thredds
------------------------------------------------------

This exercise is reading 1 km re-analysed forcing from surfex forcing files located on thredds at MET-Norway. Many points and might be slow with a poor connection, but is the best avaliable forcing we can provide and you can get it from everywhere.

.. code-block:: bash

   # You can also use forcing files from here: /hpc/perm/ms/no/sbu/training/budapest_2022_pysurfex_training_data.tgz
   # -p nobackup/trainingData/netcdf_forcing/FORCING_20220428T07Z.nc
   
   cd
   mkdir -p sfx_home
   cd sfx_home
   mkdir forcing
   cd forcing
   create_forcing 2022042803 2022042806 \
   -d [path-to-pysurfex]/examples/domains/drammen.json \
   -p https://thredds.met.no/thredds/dodsC/metusers/trygveasp/forcing/met_nordic/@YYYY@/@MM@/@DD@/FORCING_@YYYY@@MM@@DD@T@HH@Z.nc \
   --zsoro_converter none \
   -i surfex \
   --rain_converter none \
   --wind_converter none \
   --wind_dir_converter none \
   -ig [path-to-pysurfex]/examples/domains/met_nordic.json
   
E.1.1.1 Same exersice with MEPS deterministic from thredds

.. code-block:: bash

   cd
   mkdir -p sfx_home
   cd sfx_home
   mkdir forcing_meps
   cd forcing_meps

   create_forcing 2022042803 2022042806 \
   -d [path-to-pysurfex]/examples/domains/drammen.json \
   -p https://thredds.met.no/thredds/dodsC/meps25epsarchive/@YYYY@/@MM@/@DD@/meps_det_2_5km_@YYYY@@MM@@DD@T@HH@Z.nc \
   --zsoro_converter phi2m \
   -i netcdf \
   --rain_converter totalprec \
   --wind_converter windspeed \
   --wind_dir_converter winddir \
   --uval constant \
   --zval constant \
   --sca_sw constant \
   --co2 constant



E1.2: Create PREP file
-----------------------

PGD file can be fetched from sample data. See Part 3.  Assumed to be in ~/sfx_home/[NAME-OF-EXP]/PGD_DIR/.

.. code-block:: bash

   cd
   mkdir -p sfx_home/EXP
   cd sfx_home/EXP
   mkdir PREP_DIR

   # Set openMP threads
   export OMP_NUM_THREADS=1

   # Create rte.json
   dump_environ

   # run prep
   prep -c nobackup/trainingData/config_example.toml \
   -r rte.json \
   --domain [path-to-pysurfex]/examples/domains/drammen.json \
   -s system.json \
   -n [path-to-pysurfex-experiment]/nam/ \
   --pgd PGD_DIR/PGD.nc -o PREP_DIR/PREP.nc \
   --prep_file [path-to-pysurfex]/test/nam/prep_from_namelist_values.json --prep_filetype json  \
   --dtg 2022042803 \
   PREP



E1.3: Run OFFLINE
-------------------

PGD file can be fetched from sample data. See Part 3. Assumed to be in ~/sfx_home/EXP/PGD/.

.. code-block:: bash

   cd
   mkdir -p sfx_home/EXP
   cd sfx_home/EXP
   mkdir OFFLINE_DIR

   # Set openMP threads
   export OMP_NUM_THREADS=1

   # Create rte.json
   dump_environ

   # Run offline
   offline -c nobackup/trainingData/config_example.toml \
  -r rte.json \
  --domain [path-to-pysurfex]/examples/domains/drammen.json \
  -s system.json \
  -n [path-to-pysurfex-experiment]/nam/ \
  --pgd PGD_DIR/PGD.nc \
  --prep PREP_DIR/PREP.nc \
  -o OFFLINE_DIR/SURFOUT.nc \
  --forcing $PWD/forcing \
  --forc_zs \
  OFFLINE


======================================================
Part 2: Observations and surface assimilation
======================================================

Prepare screen level observations (t2m, rh2m and snow depth)

.. code-block:: bash

   cd sfx_home
   mkdir -p obsHandling
   cd obsHandling

E2.1: Create a json observation file from a bufr file

-----------------------------------------------------------

.. code-block:: bash
   
   bufr2json -b archive/observations/2022/04/28/06/ob2022042806 -v airTemperatureAt2M relativeHumidityAt2M totalSnowDepth -o ob2022042806.json -dtg 2022042806 -range 1800
      
E2.2: Create a first guess for horizontal OI
------------------------------------------------------

.. code-block:: bash

   # Create first guess netCDF file for the model equivalent variables:
   # Set paths to input and output files
   raw=FirstGuess4Gridpp.nc 
   climfile=climate/PGD.nc
   fg_ua=nobackup/trainingData/grib_FG/first_guess_gridpp_grib
   fg_sfx=nobackup/trainingData/grib_FG/first_guess_sfx_gridpp_grib
   DTG=2022042806
   
   
   FirstGuess4gridpp -dtg $DTG \
   -c nobackup/trainingData/first_guess.yml \
   -i $fg_ua \
   -if grib2 \
   -d [path-to-pysurfex]/examples/domains/drammen.json \
   -sd_file $fg_sfx \
   -sd_format grib1 \
   --sd_converter sdp \
   -altitude_file $fg_ua \
   -altitude_format grib2 \
   --altitude_converter phi2m \
   -laf_file $climfile  \
   -laf_format surfex \
   --laf_converter sea2land \
   air_temperature_2m relative_humidity_2m surface_snow_thickness \
   -o $raw

   # This creates the file FirstGuess4Gridpp.nc

E2.2: Quality control and horizontal OI
------------------------------------------------------

.. code-block:: bash

   # Quality control and optimal interpolation of the observed values
   # NB remember to set the correct paths in the config.json file!!
   
   cp nobackup/trainingData/config.json .
   
   titan --domain [path-to-pysurfex]/examples/domains/drammen.json -i config.json -dtg 2022042806 -v t2m -o qc_obs_t2m.json domain nometa redundancy plausibility fraction firstguess
   
   # This creates the file qc_obs_t2m.json, repeat the process for rh2m and sd
   
   gridpp -i FirstGuess4Gridpp.nc --obs qc_obs_t2m.json -o an_t2m.nc -v air_temperature_2m -hor 35000 -vert 200 --elevGradient -0.0065
   
   # This creates the analysis file an_t2m.nc, repeat the process for rh2m and sd

E2.3: Prepare ASCII file for SODA
------------------------------------------------------

.. code-block:: bash

   # Prepare OBSERVATIONS.dat file for Soda
   
   oi2soda --t2m_file an_t2m.nc --rh2m_file an_rh2m.nc --sd_file an_sd.nc 2022042806 -o OBSERVATIONS_220428H06.DAT
   
Prepare satellite derived soil moisture observations using pySurfex
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The next exercises are similar to the previous ones but focusing on preparing remote sensing observations of  surface soil moisture from Sentinel and demonstrate how you could do horizontal analysis on these,


.. code-block:: bash
      
   cd hm_sfx
   mkdir sentinel_sm
   cd sentinel_sm
   
   # FirstGuess4gridpp   
   # Define paths to input and output data
   raw=FirstGuess4GridppSM.nc   
   climfile=climfile=/climate/PGD.nc
   fg_ua=nobackup/trainingData/grib_FG/first_guess_gridpp_grib
   fg_sfx=nobackup/trainingData/grib_FG/first_guess_sfx_gridpp_grib
   DTG=2021060506
   
   FirstGuess4gridpp -dtg $DTG \
      -c nobackup/trainingData/first_guess.yml \
      -i $fg_ua \
      -if grib2 \
      -d [path-to-pysurfex]/examples/domains/drammen.json \
      -sm_file $fg_sfx \
      -sm_format grib1 \
      --sm_converter smp \
      -altitude_file $fg_ua \
      -altitude_format grib2 \
      --altitude_converter phi2m \
      -laf_file $climfile  \
      --laf_converter sea2land \
      -laf_format surfex \
      surface_soil_moisture \
      -o $raw



E2.4: Create an observation set
------------------------------------------------------------------------------------------------------------

.. code-block:: bash      

   # Create json file for titan and gridpp
   
   sentinel_obs --varname surface_soil_moisture -fg FirstGuess4GridppSM.nc -i /nobackup/trainingData/Sentinel_SM.nc -o sentinel_obs.json
   
E2.5: Quality control
------------------------------------------------------------------------------------------------------------

.. code-block:: bash  
   
   # Quality control of observations
   cp nobackup/trainingData/config_sentinel.json .
   
   titan --domain [path-to-pysurfex]/examples/domains/drammen.json -i config_sentinel.json -dtg 2021060506 -v surface_soil_moisture -o qc_sentinel.json domain nometa redundancy plausibility fraction firstguess

E2.6: Horizontal OI
------------------------------------------------------------------------------------------------------------

.. code-block:: bash

   # gridPP 
   
   gridpp -i FirstGuess4GridppSM.nc --obs qc_sentinel.json -o an_sm.nc -v surface_soil_moisture -hor 1000 -vert 200 --elevGradient -0.0065
 
E2.7: Prepare ASCII file for SODA
------------------------------------------------------------------------------------------------------------

.. code-block:: bash

   # Prepare OBSERVATIONS.dat file for Soda

   oi2soda --sm_file an_sm.nc 2021060506 -o OBSERVATIONS_210605H06.DAT
   

======================================================
Part 3: Running pySurfex experiment
======================================================

This part will give you guidance in how to install and setup pySurfex for your architecture of choice and some direct instructions are provided for those with access to the ECMWF dual-host system ecgate-cca.

In addition to have pySurfex available you should also install https://github.com/metno/pysurfex-scheduler.
Lastly you need  https://github.com/metno/pysurfex-experiment and you probably need to define you new host before running so I would recommend to install with "pip -e ." or clone/download the repo.
Now you have pysurfex-experiment either as clone or installed as an editable pip package.

1) Add your host
------------------------------------------------------

You are probably now doing this on a new “host”. Define a name of your host.

- config/system/[host].toml - Define your system based on other examples

  - Set SURFEX_CONFIG in Env_system to be the same as you have called your host
  - SCHEDULER_PYTHONPATH should be set on each host to whatever needed to import python modules from pysurfex-scheduler and ecflow.
- config/submit/[host].json - Look at other examples. Probably you will run a localhost setup on your laptop?
- config/input_paths/[host].json - Look at other examples. You need to set the variables you are going to use to where you have the data on your system. Typically for PGD input data.
- config/server/[host].json - Define your ecflow server name (host name on your laptop?) and port and/or port_offset
- config/env/[host].py - This file will be added to your batch script for all batch jobs. Most likely you can keep this empty.

2.) Get SURFEX source code
------------------------------------------------------

In the examples here we will use a slightly modified version of the AROME-Arctic preop2 code which has been running SEKF now for several years. You can get this code, some auxillary code and sample data from ecmwf (or ask).

.. code-block:: bash

  # AA preop code
  /hpc/perm/ms/no/sbu/training/AA_preop2_surfex_v1.tgz

  # Auxlibs (gribex still neeeded)
  /hpc/perm/ms/no/sbu/training/auxlib.tgz

  # PGD/PREP/OFFLINE/forcing/Observations
  /hpc/perm/ms/no/sbu/training/budapest_2022.tgz


Compilation is done with the OfflineNWP option. You need a file conf/system-[SURFEX_CONFIG] and src/Rules-[SURFEX_CONFIG] in the AA preop2 surfex code.


3.) Set up your experiment for your [host].
------------------------------------------------------

Adapt PATH/PYTHONPATH unless not installed in system wide locations. You should be able to import ecflow, pysurfex-scheduler and pysurfex-experiment.
You should use the offline SURFEX source code from AA preop2. Create a name of your experiment in ~/sfx_home/[EXP-NAME] and enter this directory. Setup the experiment

.. code-block:: bash

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir EXP
  cd EXP
  [pysurfex-experiment-path]/bin/PySurfexSetup \
 -experiment [pysurfex-experiment-path] \
 -surfex [pysurfex-path] \
 -host [host] \
 -offline [path-to-AA-preop2]


4.) Run your experiment!
------------------------------------------------------

Now we should be ready to start the experiment:

.. code-block:: bash

   ./bin/PySurfex start -dtg 2022042803 -dtgend 2022042806


4.1) Reconfigure your experiment
------------------------------------------------------

If you have started you experiment and you want to change the configurations without running Pysurfex start/prod. Then you can reconfigure the experiment with:

.. code-block:: bash

  ./bin/PySurfexConfig

then you can run InitRun and continue the scheduler. This command updates the json setting files picked up by the ecflow tasks.


Excercises
^^^^^^^^^^^

E3.1: Snow assimilation only on your local platform
------------------------------------------------------

.. code-block:: bash

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir SNOWASS
  cd SNOWASS
  [pysurfex-experiment-path]/bin/PySurfexSetup \
 -experiment [pysurfex-experiment-path] \
 -surfex [pysurfex-path] \
 -host [host] \
 --config_file [pysurfex-experiment-path]/config/configurations/isba_dif_snow_ass.toml \
 -offline [path-to-AA-preop2]

  # Prepare observations from the sample data in your observation directory
  # [SFX_EXP_DATA]/archive/observations/2022/04/28/06

  # Start run
  ./bin/PySurfex start -dtg 2022042803 -dtgend 2022042806

E3.2: SEKF only on your local platform
------------------------------------------------------

.. code-block:: bash

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir SEKF
  cd SEKF
  [pysurfex-experiment-path]/bin/PySurfexSetup \
 -experiment [pysurfex-experiment-path] \
 -surfex [pysurfex-path] \
 -host [host] \
 --config_file [pysurfex-experiment-path]/config/configurations/sekf.toml\
 -offline [path-to-AA-preop2]

  # Prepare observations from the sample data in your observation directory
  # [SFX_EXP_DATA]/archive/observations/2022/04/28/06

  # Start run
  ./bin/PySurfex start -dtg 2022042803 -dtgend 2022042806


E3.3: Snow assimilation only on ecgate/cca
------------------------------------------------------

Log in to ecgate

.. code-block:: bash

  module load python3/3.8.8-01
  module load ecflow/5.8.1
  export PATH=/hpc/perm/ms/no/sbu/training/pysurfex-experiment/bin/:/hpc/perm/ms/no/sbu/training/pysurfex/bin:$PATH
  export PYTHONPATH=/hpc/perm/ms/no/sbu/training/pysurfex-experiment/:/hpc/perm/ms/no/sbu/training/pysurfex-scheduler:/hpc/perm/ms/no/sbu/training/pysurfex:/hpc/perm/ms/no/sbu/training/addons/3.8.8/:$PYTHONPATH

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir SNOWASS
  cd SNOWASS
  PySurfexExpSetup -experiment /hpc/perm/ms/no/sbu/training/pysurfex-experiment \
 -host ecgb-cca \
 -surfex /hpc/perm/ms/no/sbu/training/pysurfex \
 -offline /hpc/perm/ms/no/sbu/training/AA_preop2 \
 --config_file /hpc/perm/ms/no/sbu/training/pysurfex-experiment/config/configurations/isba_dif_snow_ass.toml

  # Find your user id
  id -u
  # Replace ECF_PORT in Env_server with this number

  # Replace no with your coutry code in Env_system

  # Sample data can be found on cca under /hpc/perm/ms/no/sbu/training/budapest_2022
  # We need to prepare a couple of shot-cuts since OpenDAP does not work on CCA and I
  # have not preared other input sources (like grib files)

  # Log into cca. SFX_EXP_DATA=/scratch/ms/CC/$USER/sfx_data/SNOWASS
  # Prepare observations from the sample data in your observation directory
  # [SFX_EXP_DATA]/archive/observations/2022/04/28/06

  # Prepare forcing data from sample data
  # [SFX_EXP_DATA]/forcing

  # Prepare first guess from sample data. This could be taken from your own first guess. Maybe you can try it out?
  # archive/2022/04/28/06/raw*.nc

  # Start run
  ./bin/PySurfexExp start -dtg 2022042803 -dtgend 2022042806


E3.4: SEKF only on you local platform
------------------------------------------------------

Log in to ecgate

.. code-block:: bash

  module load python3/3.8.8-01
  module load ecflow/5.8.1
  export PATH=/hpc/perm/ms/no/sbu/training/pysurfex-experiment/bin/:/hpc/perm/ms/no/sbu/training/pysurfex/bin:$PATH
  export PYTHONPATH=/hpc/perm/ms/no/sbu/training/pysurfex-experiment/:/hpc/perm/ms/no/sbu/training/pysurfex-scheduler:/hpc/perm/ms/no/sbu/training/pysurfex:/hpc/perm/ms/no/sbu/training/addons/3.8.8/:$PYTHONPATH

  cd
  mkdir -p sfx_home
  cd sfx_home
  mkdir SEKF
  cd SEKF
  PySurfexExpSetup -experiment /hpc/perm/ms/no/sbu/training/pysurfex-experiment \
 -host ecgb-cca \
 -surfex /hpc/perm/ms/no/sbu/training/pysurfex \
 -offline /hpc/perm/ms/no/sbu/training/AA_preop2 \
 --config_file /hpc/perm/ms/no/sbu/training/pysurfex-experiment/config/configurations/sekf.toml

  # Find your user id
  id -u
  # Replace ECF_PORT in Env_server with this number

  # Replace no with your coutry code in Env_system

  # Sample data can be found on cca under /hpc/perm/ms/no/sbu/training/budapest_2022
  # We need to prepare a couple of shot-cuts since OpenDAP does not work on CCA and I
  # have not preared other input sources (like grib files)

  # Log into cca. SFX_EXP_DATA=/scratch/ms/CC/$USER/sfx_data/SEKF
  # Prepare observations from the sample data in your observation directory
  # [SFX_EXP_DATA]/archive/observations/2022/04/28/06

  # Prepare forcing data from sample data
  # [SFX_EXP_DATA]/forcing

  # Prepare first guess from sample data. This could be taken from your own first guess. Maybe you can try it out?
  # archive/2022/04/28/06/raw*.nc

  # Start run
  ./bin/PySurfexExp start -dtg 2022042803 -dtgend 2022042806
