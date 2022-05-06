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

   mkdir -p nobackup/training_data
   mkdir -p nobackup/ObHandlingOutput
   cd nobackup/training_data
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

bufr2json
=============================================================

.. code-block:: bash

   # Create observation file in json format from bufr file   
   cd hm_sfx
   mkdir obHandling
   cd obHandling
   
   bufr2json -b /nobackup/training_data/ob2021060506 -v airTemperatureAt2M relativeHumidityAt2M totalSnowDepth -o /nobackup/ObHandlingOutput/ob2021060506.json -dtg 2021060506 -range 1800
      
FirstGuess4gridpp
=============================================================   
.. code-block:: bash

   # Create first guess netCDF file for the model equivalent variables:
   # Set paths to input and output files
   raw=/nobackup/obsOp_output/FirstGuess4Gridpp.nc 
   climfile=/nobackup/training_data/Const.Clim.sfx.grib
   fg_ua=/nobackup/training_data/first_guess_gridpp_grib
   fg_sfx=/nobackup/training_data/first_guess_sfx_gridpp_grib
   DTG=2021060506

   
   FirstGuess4gridpp -dtg $DTG \
   -c /nobackup/training_data/first_guess.yml \
   -i $fg_ua \
   -if grib2 \
   -d /nobackup/training_data/drammen.json \
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






