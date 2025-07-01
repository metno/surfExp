#!/bin/bash

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 casedir ecf-dir exp domain"
else
  casedir=$1
  ecf_dir=$2
  exp=$3
  domain=$4
fi
set -x
# Touch PGD files
touch $casedir/climate/$domain/PGD_????.nc

# Clean archive
find $casedir/archive -type f -mtime +14 -print -exec rm -v "{}" +
find $casedir/archive -type d -empty -print -delete

# Clean forcing
find $casedir/forcing/?????????? -type f -mtime +14 -print -exec rm -v "{}" +
find $casedir/forcing -type d -empty -print -delete

# Clean working directories
find $casedir/????????_???? -type d -mtime +5 -print -exec rm -rv "{}" +
find $casedir/????????_???? -type d -empty -print -delete

# Clean grib files and interpolated files
find $casedir/grib/ -type f -mtime +5 -print -delete

# Clean ecflow files
find $ecf_dir/ecf_files/$exp -type f -mtime +3 -print -delete
find $ecf_dir/ecf_files/$exp -type d -empty -print -delete
find $ecf_dir/jobout/$exp -type f -mtime +3 -print -delete
find $ecf_dir/jobout/$exp -type d -empty -print -delete
