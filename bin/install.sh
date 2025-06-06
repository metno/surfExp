#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 host_file plugin_home micromamba_env_name"
  exit 1
else
  host_file=$1
  plugin_home=$2
  micromamba_env_name=$3
fi

[ ! -f $host_file ] && echo "No host_file=$host_file file found" && exit 1
. $host_file

export PATH=${micromamba_path}/bin/:$PATH
export MAMBA_ROOT_PREFIX=${micromamba_path}  # optional, defaults to ~/micromamba
eval "$(micromamba shell hook -s posix)"

set -x
micromamba env create --name $micromamba_env_name --file $plugin_home/environment.yml -y || exit 1
cd $plugin_home
micromamba activate $micromamba_env_name || exit 1
poetry install || exit 1

