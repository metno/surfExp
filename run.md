

# Todo: 
- ~~domain specific mars request~~
- Test big domain
- QC/OI with Deode observations
- merge/make namelists as deode
- setup analyse structure
- update start time
- use local config
- use binary from deode

- troika config from deode? (not from scratch)

# Other
# /lus/h2resw01/scratch/sbu/deode_virtualenvs/surfexp-ZxaY7Jni-py3.10/lib/python3.10/site-packages/cfunits/units.py
# _libpath = "/usr/local/apps/udunits/2.2.28/lib/libudunits2.so"


# Domains:
https://hirlam.github.io/nwptools/domain.html


# DRAMMEN2
time surfExp dt_offline_drammen.toml CY49DT_OFFLINE_DRAMMEN /home/sbu/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/domains/DRAMMEN2.toml surfexp/data/config/mods/dt_an_forcing.toml
deode start suite --config-file dt_offline_drammen.toml

# DT_2_5_2500x2500
time surfExp dt_offline_dt_2_5_2500x2500.toml CY49DT_OFFLINE_dt_2_5_2500x2500 /home/sbu/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/domains/dt_2_5_2500x2500.toml surfexp/data/config/mods/dt_an_forcing.toml
deode start suite --config-file dt_offline_dt_2_5_2500x2500.toml
