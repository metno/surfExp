

## Todo: 
- ~~domain specific mars request~~
- Test big domain
- ~QC/OI with Deode observations~
- merge/make namelists as deode
- ~setup analyse structure~
- update start time
- ~use local config~
- ~use binary from deode~
- set ecflow limit in suite for number of tasks

- troika config from deode? (not from scratch)

## Other
 - ~fix cfunits installation for ATOS-Bologna~


# Domains:
[HIRLAM domain generator](https://hirlam.github.io/nwptools/domain.html)


# Install and run

## Clone and checkout

```
git clone git@github.com:trygveasp/surfExp
cd surfExp
git checkout -b deode_offline_surfex_workflow origin/deode_offline_surfex_workflow
```

### Possible local changes (install on scratch for speed /scratch/$USER/deode_virtualenvs)

```
diff --git a/poetry.toml b/poetry.toml
index fe20193..8333d82 100644
--- a/poetry.toml
+++ b/poetry.toml
@@ -1,6 +1,8 @@
 [virtualenvs]
   create = true
-  in-project = true
+  in-project = false
+  path = "/scratch/sbu/deode_virtualenvs/"
+  prefer-active-python = true
```

## Install and activate environment (see also Deode-Workflow documentation)

```
poetry install
poetry shell
```

## Create and start experiments

### South-Norway domain
```
time surfExp dt_offline_drammen.toml CY49DT_OFFLINE_DRAMMEN /home/$USER/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/domains/DRAMMEN2.toml surfexp/data/config/mods/dt_an_forcing.toml
time deode start suite --config-file dt_offline_drammen.toml
```

### Extendended domain over Denmark
```
time surfExp edenmark.toml EDENMARK /home/$USER/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/include/domains/EDENMARK.toml surfexp/data/config/mods/dt_an_forcing.toml surfexp/data/config/mods/prep.toml 
time deode start suite --config-file edenmark.toml
```

### North-central EUROPE
```
time surfExp nc_europe.toml NC_EUROPE /home/$USER/projects/surfExp2 surfexp/data/config/configurations/dt.toml surfexp/data/config/include/domains/NC_EUROPE.toml surfexp/data/config/mods/dt_an_forcing.toml surfexp/data/config/mods/prep.toml
time deode start suite --config-file nc_europe.toml 
```

### DT_2_5_2500x2500
```time surfExp dt_offline_dt_2_5_2500x2500.toml CY49DT_OFFLINE_dt_2_5_2500x2500 /home/$USER/projects/surfExp surfexp/data/config/configurations/dt.toml surfexp/data/config/domains/dt_2_5_2500x2500.toml surfexp/data/config/mods/dt_an_forcing.toml
time deode start suite --config-file dt_offline_dt_2_5_2500x2500.toml
```
