#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 scratch ecf-dir"
else
  scratch=$1
  ecf_dir=$2
fi
set -x
# Touch PGD files
touch $scratch/climate/DT_2_5_2500x2500/PGD_????.nc

# Touch forcing files
find $scratch/forcing/ -name "FORCING.nc" -print -exec touch {} +

# Touch archive files
find $scratch/archive -type f -print -exec touch {} +

# Clean working directories
find $scratch/????????_???? -type d -mtime +10 -print -exec rm -rv "{}" +
find $scratch/????????_???? -type d -empty -print -delete

# Clean grib files and interpolated files
find $scratch/grib/ -type f -mtime +10 -print -delete

find $ecf_dir/ecf_files/CY49DT_OFFLINE_dt_2_5_2500x2500 -type f -mtime +3 -print -delete
find $ecf_dir/ecf_files/CY49DT_OFFLINE_dt_2_5_2500x2500 -type d -empty -print -delete
find $ecf_dir/jobout/CY49DT_OFFLINE_dt_2_5_2500x2500 -type f -mtime +3 -print -delete
find $ecf_dir/jobout/CY49DT_OFFLINE_dt_2_5_2500x2500 -type d -empty -print -delete
