#!/bin/bash

if [ $# -ne 3 -a $# -ne 5 -a $# -ne 6 ]; then
  echo "Usage: $0 host-file plugin_home micromamba_env_name [prep iso-date [iso-end-date]]"
  echo "$0 $PWD/envs/ATOS-Bologna $PWD `basename $PWD` false 2025-01-01T00:00:00Z 2025-01-02T00:00:00Z"
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
  do_prep="false"
  [ $# -gt 3 ] && do_prep="$4"
  if [ $# -gt 4 ]; then
    start_time=$5
  else
    start_time=`date -d "today" '+%Y-%m-%d'`"T00:00:00Z"
    [ "$USER" == "sbu" ] && start_time=`date -d "2 day ago" '+%Y-%m-%d'`"T00:00:00Z"
  fi
  end_time=$start_time
  if [ $# -gt 5 ]; then
    end_time=$6
  fi
fi

# Experiment
exp="CY49DT_OFFLINE_dt_2_5_2500x2500"

# Platform specific variables
[ "$scratch" == "" ] && echo "scratch not set!" && exit 1
[ "$ecf_dir" == "" ] && echo "ecf_dir not set!" && exit 1
[ "$binaries_opt" == "" ] && echo "binaries_opt not set!" && exit 1
[ "$binaries_de" == "" ] && echo "binaries_de not set!" && exit 1
[ "$micromamba_path" == "" ] && echo "micromamba_path not set!" && exit 1

# Experiment specific
config="dt_offline_dt_2_5_2500x2500_running.toml"
domain="surfexp/data/config/domains/dt_2_5_2500x2500.toml"
domain_name="DT_2_5_2500x2500"

# Staging environment
if [ "$USER" == "sbu" ]; then
  config="dt_offline_dt_2_5_50x60_running.toml"
  domain="surfexp/data/config/domains/DRAMMEN.toml"
  domain_name="DRAMMEN"
  exp="CY49DT_OFFLINE_dt_2_5_50x60"
fi
# Micromamba
export PATH=${micromamba_path}/bin/:$PATH
export MAMBA_ROOT_PREFIX=${micromamba_path}  # optional, defaults to ~/micromamba
eval "$(micromamba shell hook -s posix)"

micromamba activate $micromamba_env_name || exit 1

set -x
cd $plugin_home

# Archive previous run
$plugin_home/bin/archive_ecfs.sh "$scratch/surfexp/$exp" "$start_time" "$exp" || exit 1

# Clean
$plugin_home/bin/clean.sh "$scratch/surfexp/$exp" "$ecf_dir" "$exp" "$domain_name"

mods="mods_run.toml"
cat > $mods << EOF
[general]
  max_tasks = 60

[general.times]
  start = "$start_time"
  end = "$end_time"

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
  create_static_data = false
  create_time_dependent_suite = true
  do_archiving = true
  do_cleaning = true
  do_extractsqlite = true
  do_marsprep = true
  do_pgd = false
  do_PrefetchMars = true
  do_prep = $do_prep

[submission]
  bindir = "$binaries_de"
[submission.task_exceptions.Forecast]
  bindir = "$binaries_de"
[submission.task_exceptions.Pgd]
  bindir = "$binaries_de"
[submission.task_exceptions.Prep]
  bindir = "$binaries_opt"
[submission.task_exceptions.QualityControl.MODULES]
  PRGENV = ["load", "prgenv/gnu"]

# HRES
[mars.an_forcing]
  config = "sfx_hres"

[mars.default]
  config = "sfx_hres"

[mars.sfx_hres]
  class = "OD"
  expver = "1"
  grid = "0.04/0.04"

[forcing.args.default]
  pattern = "@casedir@/grib/an_forcing/sfx_hres_@YYYY@@MM@@DD@@HH@+@LL@.nc"

[initial_conditions.fg4oi.an_forcing.rh2m]
  t-inputfile = "@casedir@/grib/an_forcing/sfx_hres_@YYYY@@MM@@DD@@HH@+@LL@.nc"
  td-inputfile = "@casedir@/grib/an_forcing/sfx_hres_@YYYY@@MM@@DD@@HH@+@LL@.nc"

[initial_conditions.fg4oi.an_forcing.t2m]
  inputfile = "@casedir@/grib/an_forcing/sfx_hres_@YYYY@@MM@@DD@@HH@+@LL@.nc"

EOF

time surfExp -o $config \
--case-name $exp \
--plugin-home $plugin_home  \
--troika troika \
surfexp/data/config/configurations/dt.toml \
$domain \
surfexp/data/config/mods/dev-CY49T2h_deode/dt.toml \
surfexp/data/config/mods/dev-CY49T2h_deode/dt_prep_from_namelist.toml \
$mods \
--start-time $start_time \
--end-time $end_time

time deode start suite --config-file $config || exit 1

