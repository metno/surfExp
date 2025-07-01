#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 casedir dtg exp"
  exit 1
else
  casedir=$1
  dtg=$2
  exp=$3
fi
set -x

# 1234567890123456790
# 2025-06-30T00:00:00Z
dtg=`echo $dtg | cut -c1-4`"-"`echo $dtg | cut -c6-7`"-"`echo $dtg | cut -c9-10`" "`echo $dtg | cut -c12-13`":00"
fg_dtg=`date -u --date "$dtg 2 day ago" +%Y%m%d%H`

yyyy=`echo $fg_dtg | cut -c1-4`
mm=`echo $fg_dtg | cut -c5-6`
dd=`echo $fg_dtg | cut -c7-8`
hh=`echo $fg_dtg | cut -c9-10`

archive_path=archive/$yyyy/$mm/$dd/$hh
els ec:surfexp/$exp/$archive_path
[ $? -eq 1 ] && emkdir -p ec:surfexp/$exp/$archive_path
els ec:surfexp/$exp/$archive_path || exit 1

forcing_path=forcing/$fg_dtg/an_forcing
forcing_file=$casedir/$forcing_path/FORCING.nc
[ ! -f $forcing_file ] && forcing_file=$casedir/forcing/$fg_dtg/FORCING.nc
ls -l $forcing_file
els ec:surfexp/$exp/$archive_path/FORCING.nc
if [ $? -eq 1 ]; then
  time ecp $forcing_file ec:surfexp/$exp/$archive_path/FORCING.nc || exit 1
fi

output_file=$casedir/$archive_path/SURFOUT.nc
ls -l $output_file
els ec:surfexp/$exp/$archive_path/SURFOUT.nc
if [ $? -eq 1 ]; then
  time ecp $output_file ec:surfexp/$exp/$archive_path/SURFOUT.nc || exit 1
fi

