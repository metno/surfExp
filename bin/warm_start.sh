#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 FIRST_GUESS_DTG"
  exit 1
else
  dtg=$1
fi

new_exp_scratch="/scratch/$USER/deode/CY49DT_OFFLINE_dt_2_5_2500x2500/"
old_exp_scratch="/scratch/snh02/deode/CY49DT_OFFLINE_dt_2_5_2500x2500/"

set -x
# Climate data
climpath="climate/DT_2_5_2500x2500"
mkdir -p $new_exp_scratch/$climpath
for f in `ls -1 $old_exp_scratch/$climpath/PGD_*.nc`; do
  ln -s $f $new_exp_scratch/$climpath/.
done

yyyy=`echo $dtg | cut -c 1-4`
mm=`echo $dtg | cut -c 5-6`
dd=`echo $dtg | cut -c 7-8`
hh=`echo $dtg | cut -c 9-10`

archive="archive/$yyyy/$mm/$dd/$hh/"
mkdir -p $new_exp_scratch/$archive
for f in `ls -1 $old_exp_scratch/$archive/SURFOUT.nc`; do
  cp $f $new_exp_scratch/$archive/.
done

