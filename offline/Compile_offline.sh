#!/bin/bash

if [ $# -ne 2 -a $# -ne 3 ]; then
  echo "Usage: $0 offline arch [threads]"
  exit 1
fi
offline=$1
arch=$2
threads=4
if [ $# -eq 3 ]; then
  threads=$3
fi

set -x
[ ! -d $offline ] && echo "Offline direcory $offline does not exist" && exit 1
. ${offline}/conf/system.${arch}
. ${offline}/conf/profile_surfex-${arch}
cd $offline/src || exit 1
make -j $threads  || exit 1

