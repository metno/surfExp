#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage: $0 offline arch"
  exit 1
fi
offline=$1
arch=$2

export OFFLINE_CONFIG=$arch
[ ! -d $offline ] && echo "Offline direcory $offline does not exist" && exit 1
. ${offline}/conf/system.${arch} || exit 1
cd $offline/src || exit 1
./configure OfflineNWP ${offline}/conf/system.${arch} || exit 1

