=========================================
 ACCORD training Budapest May 9-13 20222
=========================================

pySurfex command line exercises
=======================================================

- Assume pysurfex is installed and in your path
- Assume pysurfex installation directory is "path-to-pysurfex"
- Assume that you have your surfex binaries in PATH and that they are called PGD, PREP, OFFLINE and SODA
- Assume that you have system paths defined in a file called system.json
- Assume a domain defined in a file called domain.json

Outline
=======================================================
- create offline forcing from netCDF and grib files using pySurfex create_forcing
- create PREP file
- Run OFFLINE
- Prepare screen level and satellite observations for assimilation in Soda

Create offline forcing
=======================================================
.. code-block:: bash

   mkdir hm_sfx
   cd hm_sfx
   mkdir forcing
   cd forcing
   create_forcing 2021060500 2021060503 -d examples/domains/drammen.json -p forcingdir/FORCING_@YYYY@@MM@@DD@T@HH@Z.nc --zsoro_converter none -i surfex --rain_converter none --wind_converter none --wind_dir_converter none -ig examples/domains/met_nordic.json


Create PREP file
=======================================================



Run OFFLINE
=======================================================



Prepare screen level observations (t2m, rh2m and snow depth)
=============================================================
.. code-block:: bash

   mkdir -p nobackup/trainingData
   mkdir -p nobackup/obsOutput
   cd nobackup/trainingData
   untar ObHandling.tar
   #ob2021060506 (bufr file)
   #first_guess_gridpp_grib (atmosphere FG file)
   #first_guess_sfx_gridpp_grib (surface FG file)
   #Const.Clim.sfx.grib
   #PGD.nc
   #Sentinel_2021060506.nc
   #first_guess.yml
   #config.json
   #config_sentinel.json
   #drammen.json

bufr2json
=============================================================

.. code-block:: bash

   # Create observation file in json format from bufr file   
   cd hm_sfx
   mkdir obHandling
   cd obHandling
   
   bufr2json -b /nobackup/trainingData/ob2021060506 -v airTemperatureAt2M relativeHumidityAt2M totalSnowDepth -o /nobackup/ObHandlingOutput/ob2021060506.json -dtg 2021060506 -range 1800
      
FirstGuess4gridpp
=============================================================   
.. code-block:: bash

   # Create first guess netCDF file for the model equivalent variables:
   # Set paths to input and output files
   raw=/nobackup/obsOutput/FirstGuess4Gridpp.nc 
   climfile=/nobackup/trainingData/Const.Clim.sfx.grib
   fg_ua=/nobackup/trainingData/first_guess_gridpp_grib
   fg_sfx=/nobackup/trainingData/first_guess_sfx_gridpp_grib
   DTG=2021060506

   
   FirstGuess4gridpp -dtg $DTG \
   -c /nobackup/trainingData/first_guess.yml \
   -i $fg_ua \
   -if grib2 \
   -d /nobackup/trainingData/drammen.json \
   -sd_file $fg_sfx \
   -sd_format grib1 \
   --sd_converter sdp \
   -altitude_file $fg_ua \
   -altitude_format grib2 \
   --altitude_converter phi2m \
   -laf_file $climfile  \
   -laf_format grib1 \
   --laf_converter sea2land \
   air_temperature_2m relative_humidity_2m surface_snow_thickness \
   -o $raw || exit 1

   # This creates the file FirstGuess4Gridpp.nc

titan and gridPP
=============================================================   
.. code-block:: bash

   # Quality control and optimal interpolation of the observed values
   # NB remember to set the correct paths in the config.json file!!
   
   titan --domain /nobackup/trainingData/drammen.json -i config.json -dtg 2021060506 -v rh2m -o /nobackup/obsOutput/qc_obs_rh2m.json domain nometa redundancy plausibility fraction firstguess
   
   # This creates the file qc_obs_t2m.json, repeat the process for rh2m and sd
   
   gridpp -i /nobackup/obsOp_output/FirstGuess4Gridpp.nc --obs /nobackup/obsOutput/qc_obs_t2m.json -o /nobackup/obsOutput/an_t2m.nc -v air_temperature_2m -hor 35000 -vert 200 --elevGradient -0.0065
   
   # This creates the analysis file an_t2m.nc, repeat the process for rh2m and sd

oi2soda
=============================================================   
.. code-block:: bash
   # Prepare OBSERVATIONS.dat file for Soda
   
   oi2soda --t2m_file an_t2m.nc --rh2m_file an_rh2m.nc --sd_file an_sd.nc 2021060506 -o OBSERVATIONS_210605H06.DAT
   
Prepare satellite derived soil moisture observations using pySurfex
====================================================================
.. code-block:: bash
      
   # FirstGuess4gridpp   
   # Define paths to input and output data
   raw=/nobackup/obsOp_output/FirstGuess4GridppSM.nc   
   climfile=/nobackup/trainingData/PGD.nc
   fg_ua=/nobackup/trainingData/first_guess_gridpp_grib
   fg_sfx=/nobackup/trainingData/first_guess_sfx_gridpp_grib
   DTG=2021060506
   
   FirstGuess4gridpp -dtg $DTG \
      -c /nobackup/trainingData/first_guess.yml \
      -i $fg_ua \
      -if grib2 \
      -d /nobackup/trainingData/drammen.json \
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
      -o $raw || exit 1

sentinel_obs
====================================================================
.. code-block:: bash      

   # Create json file for titan and gridpp
   
   sentinel_obs --varname surface_soil_moisture -fg /nobackup/obsOutput/FirstGuess4GridppSM.nc -i /nobackup/trainingData/Sentinel_SM_20210606.nc -o sentinel_obs.json
   
titan
====================================================================
.. code-block:: bash  
   
   # Quality control of observations
   
   titan --domain /nobackup/trainingData/drammen.json -i /nobackup/obsOutput/config_sentinel.json -dtg 2021060506 -v surface_soil_moisture -o /nobackup/obsOutput/qc_sentinel.json domain nometa redundancy plausibility fraction firstguess

gridPP
====================================================================
.. code-block:: bash

   # gridPP 
   
   gridpp -i /nobackup/obsOutput/FirstGuess4GridppSM.nc --obs /nobackup/obsOutput/qc_sentinel.json -o /nobackup/obsOutput/an_sm.nc -v surface_soil_moisture -hor 1000 -vert 200 --elevGradient -0.0065
 
oi2Soda
====================================================================
.. code-block:: bash

   oi2soda --t2m_file /nobackup/obsOutput/an_t2m.nc --sm_file /nobackup/obsOutput/an_sm.nc 2021060506 -o OBSERVATIONS_210605H06.DAT
   

