#!/bin/bash

if [ $# -ne 3 ]; then
  echo "Usage: $0 offline arch xyz-file"
  exit 1
fi
offline=$1
arch=$2
xyz_file=$3

set -x
[ ! -d $offline ] && echo "Offline direcory $offline does not exist" && exit 1
. ${offline}/conf/system.${arch}
. ${offline}/conf/profile_surfex-${arch}
echo $XYZ > $xyz_file
