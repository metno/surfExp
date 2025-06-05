#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 scratch"
else
  scratch=$1
fi
set -x
# Touch PGD files
touch $scratch/climate/DT_2_5_2500x2500/PGD_????.nc

# Touch forcing files
find $scratch/forcing/ -name "FORCING.nc" -print -exec touch {} +

# Touch arcive files
find $scratch/archive -type f -print -exec touch {} +

# Clean working directories
find $scratch/????????_???? -type d -mtime +10 -print -exec rm -rv "{}" +

# Clean grib files and interpolated files
find $scratch/grib/ -type f -mtime +10 -print -delete

