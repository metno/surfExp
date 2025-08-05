#!/bin/bash

if [ $# -ne 3 ]; then
  echo "Usage: $0 host-file plugin_home micromamba_env_name"
  echo "$0 $PWD/envs/ATOS-Bologna $PWD `basename $PWD`"
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

start_time="2025-01-01T00:00:00Z"
end_time="2026-01-31T00:00:00Z"


# Experiment
exp="CY49DT_OFFLINE_dt_2_5_2500x2500"

# Platform specific  ATOS snh02
[ "$plugin_home" == "" ] && echo "plugin_home not set!" && exit 1
[ "$ecf_dir" == "" ] && echo "ecf_dir not set!" && exit 1
[ "$scratch" == "" ] && echo "scratch not set!" && exit 1
[ "$binaries_opt" == "" ] && echo "binaries_opt not set!" && exit 1
[ "$binaries_de" == "" ] && echo "binaries_de not set!" && exit 1
[ "$micromamba_path" == "" ] && echo "micromamba_path not set!" && exit 1
[ "$micromamba_env_name" == "" ] && echo "micromamba_env_name not set!" && exit 1

# Experiment specific
config="dt_offline_dt_2_5_2500x2500_climate.toml"
domain="surfexp/data/config/domains/dt_2_5_2500x2500.toml"

# Staging environment
if [ "$USER" == "sbu" ]; then
  config="dt_offline_dt_2_5_50x60_climate.toml"
  domain="surfexp/data/config/domains/DRAMMEN.toml"
  exp="CY49DT_OFFLINE_dt_2_5_50x60"
fi

export PATH=${micromamba_path}/bin/:$PATH
export MAMBA_ROOT_PREFIX=${micromamba_path}  # optional, defaults to ~/micromamba
eval "$(micromamba shell hook -s posix)"

micromamba activate $micromamba_env_name || exit 1

set -x
cd $plugin_home

mods="mods_climate.toml"
cat > $mods << EOF
[general]
  max_tasks = 60

[general.times]
  start = "$start_time"
  end = "$end_time"
  cycle_length = "PT96H"

[system]
   casedir = "$scratch/surfexp/@CASE@"

[platform]
   scratch = "$scratch"

[scheduler.ecfvars]
  ecf_files = "$ecf_dir/ecf_files"
  ecf_files_remotely = "$ecf_dir/ecf_files"
  ecf_home = "$ecf_dir/jobout"
  ecf_jobout = "$ecf_dir/jobout"
  ecf_out = "$ecf_dir/jobout"

[suite_control]
  create_time_dependent_suite = false

[submission]
  bindir = "$binaries_de"
[submission.task_exceptions.Pgd]
  bindir = "$binaries_de"

EOF

time surfExp -o $config \
--case-name $exp \
--plugin-home $plugin_home  \
--troika troika \
surfexp/data/config/configurations/dt.toml \
$domain \
surfexp/data/config/mods/dev-CY49T2h_deode/dt.toml \
$mods \
--start-time $start_time \
--end-time $end_time

time deode start suite --config-file $config || exit 1

