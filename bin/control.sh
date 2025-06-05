#!/bin/bash

if [ $# -ne 3 ]; then
  echo "Usage: $0 host-file plugin_home micromamba_env_name"
  exit 1
else
  echo
  echo "##################################################################" 
  date
  echo "##################################################################" 
  echo

  host_file=$1
  [ ! -f $host_file ] && echo "No $host file found" && exit 1
  . $host_file

  plugin_home=$2
  micromamba_env_name=$3
fi

# Experiment
exp="CY49DT_OFFLINE_dt_2_5_2500x2500_control"

# Platform specific variables
[ "$scratch" == "" ] && echo "scratch not set!" && exit 1
[ "$binaries_opt" == "" ] && echo "binaries_opt not set!" && exit 1
[ "$binaries_de" == "" ] && echo "binaries_de not set!" && exit 1
[ "$micromamba_path" == "" ] && echo "micromamba_path not set!" && exit 1

# Experiment specific
config="dt_offline_dt_2_5_2500x2500_control.toml"
domain="surfexp/data/config/domains/dt_2_5_2500x2500.toml"

# Micromamba
export PATH=${micromamba_path}/bin/:$PATH
export MAMBA_ROOT_PREFIX=${micromamba_path}  # optional, defaults to ~/micromamba
eval "$(micromamba shell hook -s posix)"

micromamba activate $micromamba_env_name || exit 1

cd $plugin_home
echo $PATH

mods="mods_control.toml"
cat > $mods << EOF

[scheduler.ecfvars]
  ecf_files = "/perm/@USER@/deode_ecflow/ecf_files"
  ecf_files_remotely = "/perm/@USER@/deode_ecflow/ecf_files"
  ecf_home = "/perm/@USER@/deode_ecflow/jobout"
  ecf_jobout = "/perm/@USER@/deode_ecflow/jobout"
  ecf_out = "/perm/@USER@/deode_ecflow/jobout"

EOF

time surfExp -o $config \
--case-name $exp \
--plugin-home $plugin_home  \
--troika troika \
surfexp/data/config/configurations/dt.toml \
surfexp/data/config/configurations/dt_control.toml \
surfexp/data/config/domains/dt_2_5_2500x2500.toml \
surfexp/data/config/mods/dev-CY49T2h_deode/dt.toml \
$mods

time deode start suite --config-file $config || exit 1

