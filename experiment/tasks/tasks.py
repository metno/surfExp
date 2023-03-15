"""General task module."""
from datetime import timedelta, datetime
import os
import json
import shutil
import logging
import numpy as np
import yaml
import surfex


from ..toolbox import FileManager
from ..logs import get_logger_from_config


class AbstractTask(object):
    """General abstract task to be implemented by all tasks using default container."""

    def __init__(self, config):
        """Initialize a task run by the default ecflow container.

        All tasks implelementing this base class will work with the default ecflow container

        Args:

        """
        if surfex is None:
            raise Exception("Surfex module not properly loaded!")

        logging.debug("Create task")
        self.config = config
        self.logger = get_logger_from_config(config)
        self.exp_file_paths = config.exp_file_paths
        system = config.system
        dtg = config.progress.dtg
        dtgbeg = config.progress.dtgbeg
        self.dtg = dtg
        self.dtgbeg = dtgbeg

        self.host = "0"
        # self.exp_file_paths = surfex.SystemFilePaths(exp_file_paths)
        self.work_dir = self.exp_file_paths.get_system_path("exp_dir")
        self.lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        self.stream = config.get_setting("GENERAL#STREAM")
        self.surfex_config = system.get_var("SURFEX_CONFIG", self.host)
        self.sfx_exp_vars = None
        logging.debug("        config: %s", json.dumps(config.settings, sort_keys=True, indent=2))
        logging.debug("        system: %s", json.dumps(system.system, sort_keys=True, indent=2))
        logging.debug("exp_file_paths: %s", json.dumps(self.exp_file_paths.system_file_paths,
                                                       sort_keys=True, indent=2))

        self.mbr = config.ensmbr
        self.members = config.members

        # Domain/geo
        domain = self.config.get_setting("GEOMETRY#DOMAIN")
        domains = self.config.get_setting("GEOMETRY#DOMAINS")
        domain_json = surfex.set_domain(domains, domain, hm_mode=True)
        geo = surfex.get_geo_object(domain_json)
        self.geo = geo

        self.fmanager = FileManager(config)
        self.platform = self.fmanager.platform
        wrapper = self.config.get_setting("TASK#WRAPPER")
        if wrapper is None:
            wrapper = ""
        self.wrapper = wrapper

        masterodb = False
        lfagmap = self.config.get_setting("SURFEX#IO#LFAGMAP")
        self.csurf_filetype = self.config.get_setting("SURFEX#IO#CSURF_FILETYPE")
        self.suffix = surfex.SurfFileTypeExtension(self.csurf_filetype, lfagmap=lfagmap,
                                                   masterodb=masterodb).suffix

        # TODO Move to config
        ###########################################################################
        self.wrk = self.exp_file_paths.get_system_path("wrk_dir", default_dir="default_wrk_dir", basedtg=self.dtg)
        self.archive = self.exp_file_paths.get_system_path("archive_dir", default_dir="default_archive_dir",
                                                           basedtg=self.dtg)
        os.makedirs(self.archive, exist_ok=True)
        self.bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")

        self.extrarch = self.exp_file_paths.get_system_path("extrarch_dir", default_dir="default_extrarch_dir",
                                                            basedtg=self.dtg)
        os.makedirs(self.extrarch, exist_ok=True)
        self.obsdir = self.exp_file_paths.get_system_path("obs_dir", default_dir="default_obs_dir",
                                                          basedtg=self.dtg)

        self.exp_file_paths.add_system_file_path("wrk_dir", self.wrk)
        self.exp_file_paths.add_system_file_path("bin_dir", self.bindir)
        self.exp_file_paths.add_system_file_path("archive_dir", self.archive)
        self.exp_file_paths.add_system_file_path("extrarch_dir", self.extrarch)
        self.exp_file_paths.add_system_file_path("obs_dir", self.obsdir)
        os.makedirs(self.obsdir, exist_ok=True)
        self.fcint = self.config.get_fcint(self.dtg)
        self.fgint = self.config.get_fgint(self.dtg)
        self.fg_dtg = self.dtg - timedelta(seconds=self.fgint)
        self.next_dtg = self.dtg + timedelta(seconds=self.fcint)
        self.next_dtgpp = self.next_dtg
        self.first_guess_dir = self.exp_file_paths.get_system_path("first_guess_dir",
                                                                   default_dir="default_first_guess_dir",
                                                                   basedtg=self.fg_dtg)
        self.input_path = self.config.get_setting("GENERAL#NAMELIST_DIR")
        ###########################################################################

        self.wdir = str(os.getpid())
        self.wdir = self.wrk + "/" + self.wdir
        logging.info("WDIR=" + self.wdir)
        os.makedirs(self.wdir, exist_ok=True)
        os.chdir(self.wdir)

        self.fg_guess_sfx = self.wrk + "/first_guess_sfx"
        self.fc_start_sfx = self.wrk + "/fc_start_sfx"

        self.translation = {
            "t2m": "air_temperature_2m",
            "rh2m": "relative_humidity_2m",
            "sd": "surface_snow_thickness"
        }
        # self.obs_types = self.config.get_setting("SURFEX#ASSIM#OBS#COBS_M")
        self.obs_types = []
        self.nnco = self.config.get_nnco(dtg=self.config.progress.dtg)
        self.config.update_setting("SURFEX#ASSIM#OBS#NNCO", self.nnco)
        logging.debug("NNCO: %s", self.nnco)

    def run(self):
        """Run task.

        Define run sequence.

        """
        self.execute()
        self.postfix()

    def execute(self):
        """Do nothing for base execute task."""
        logging.warning("Using empty base class execute")

    def postfix(self):
        """Do default postfix.

        Default is to clean.

        """
        logging.info("Base class postfix")
        if self.wrk is not None:
            os.chdir(self.wrk)

        if self.wdir is not None:
            shutil.rmtree(self.wdir)

'''
class Dummy(object):
    """A dummy task to test the containers.

    Args:
        object (_type_): _description_
    """

    def __init__(self, config):
        """Construct the Dummy task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        exp_file_paths = config["SYSTEM_FILE_PATHS"]
        system = config["SYSTEM_VARS"]
        progress = config["PROGRESS"]
        logging.debug("Dummy task initialized")
        logging.debug("        Config: %s", json.dumps(config, sort_keys=True, indent=2))
        logging.debug("        system: %s", json.dumps(system, sort_keys=True, indent=2))
        logging.debug("exp_file_paths: %s", json.dumps(exp_file_paths, sort_keys=True, indent=2))
        logging.debug("      progress: %s", json.dumps(progress, sort_keys=True, indent=2))

    def run(self):
        """Override run."""
        logging.debug("Dummy task %s is run")
'''

class PrepareCycle(AbstractTask):
    """Prepare for th cycle to be run.

    Clean up existing directories.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the PrepareCycle task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, config)

    def run(self):
        """Override run."""
        self.execute()

    def execute(self):
        """Execute."""
        if os.path.exists(self.wrk):
            shutil.rmtree(self.wrk)


class QualityControl(AbstractTask):
    """Perform quakity control of observations.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Constuct the QualityControl task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, config)
        print(self.config.get_setting("TASK"))
        self.var_name = self.config.get_setting("TASK#VAR_NAME")
        print(self.var_name)

    def execute(self):
        """Execute."""
        an_time = self.dtg

        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")

        fg_file = self.exp_file_paths.get_system_file("archive_dir", "raw.nc", basedtg=self.dtg,
                                                      default_dir="default_archive_dir")

        # Default
        settings = {
            "domain": {
                "domain_file": sfx_lib + "/domain.json"
            },
            "firstguess": {
                "fg_file": fg_file,
                "fg_var": self.translation[self.var_name]
            }
        }
        default_tests = {
            "nometa": {
                "do_test": True
            },
            "domain": {
                "do_test": True,
            },
            "blacklist": {
                "do_test": True
            },
            "redundancy": {
                "do_test": True
            }
        }

        # T2M
        if self.var_name == "t2m":
            synop_obs = self.config.get_setting("OBSERVATIONS#SYNOP_OBS_T2M")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update({
                    "plausibility": {
                        "do_test": True,
                        "maxval": 340,
                        "minval": 200
                    }
                })
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update({
                    "bufr": {
                        "filepattern": filepattern,
                        "filetype": "bufr",
                        "varname": "airTemperatureAt2M",
                        "tests": bufr_tests
                    }
                })
            netatmo_obs = self.config.get_setting("OBSERVATIONS#NETATMO_OBS_T2M")
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update({
                    "sct": {
                        "do_test": True
                    },
                    "plausibility": {
                        "do_test": True,
                        "maxval": 340,
                        "minval": 200
                    }
                })
                filepattern = self.config.get_setting("OBSERVATIONS#NETATMO_FILEPATTERN",
                                                      check_parsing=False)
                data_sets.update({
                    "netatmo": {
                        "filepattern": filepattern,
                        "varname": "Temperature",
                        "filetype": "netatmo",
                        "tests": netatmo_tests
                    }
                })

            settings.update({
                "sets": data_sets
            })

        # RH2M
        elif self.var_name == "rh2m":
            synop_obs = self.config.get_setting("OBSERVATIONS#SYNOP_OBS_RH2M")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update({
                    "plausibility": {
                        "do_test": True,
                        "maxval": 100,
                        "minval": 0
                    }
                })
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update({
                    "bufr": {
                        "filepattern": filepattern,
                        "filetype": "bufr",
                        "varname": "relativeHumidityAt2M",
                        "tests": bufr_tests
                    }
                })

            netatmo_obs = self.config.get_setting("OBSERVATIONS#NETATMO_OBS_RH2M")
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update({
                    "sct": {
                        "do_test": True
                    },
                    "plausibility": {
                        "do_test": True,
                        "maxval": 10000,
                        "minval": 0
                    }
                })
                filepattern = self.config.get_setting("OBSERVATIONS#NETATMO_FILEPATTERN",
                                                      check_parsing=False)
                data_sets.update({
                    "netatmo": {
                        "filepattern": filepattern,
                        "varname": "Humidity",
                        "filetype": "netatmo",
                        "tests": netatmo_tests
                    }
                })

            settings.update({
                "sets": data_sets
            })

        # Snow Depth
        elif self.var_name == "sd":
            synop_obs = self.config.get_setting("OBSERVATIONS#SYNOP_OBS_SD")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update({
                    "plausibility": {
                        "do_test": True,
                        "maxval": 1000,
                        "minval": 0
                    },
                    "firstguess": {
                        "do_test": True,
                        "negdiff": 0.5,
                        "posdiff": 0.5
                    }
                })
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update({
                    "bufr": {
                        "filepattern": filepattern,
                        "filetype": "bufr",
                        "varname": "totalSnowDepth",
                        "tests": bufr_tests
                    }
                })

            settings.update({
                "sets": data_sets
            })
        else:
            raise NotImplementedError

        logging.debug("Settings %s", json.dumps(settings, indent=2, sort_keys=True))

        output = self.obsdir + "/qc_" + self.translation[self.var_name] + ".json"
        uname = self.var_name.upper()
        print(uname)
        print(f"OBSERVATIONS#QC#{uname}#TESTS")
        tests = self.config.get_setting(f"OBSERVATIONS#QC#{uname}#TESTS")
        print(tests)
        if tests is None:
            logging.info("Use default test OBSERVATIONS#QC#TESTS")
            tests = self.config.get_setting("OBSERVATIONS#QC#TESTS")

        indent = 2
        blacklist = {}
        json.dump(settings, open("settings.json", mode="w", encoding="utf-8"), indent=2)
        tests = surfex.titan.define_quality_control(tests, settings, an_time, domain_geo=self.geo,
                                                    blacklist=blacklist)

        datasources = surfex.obs.get_datasources(an_time, settings["sets"])
        data_set = surfex.TitanDataSet(self.var_name, settings, tests, datasources, an_time)
        data_set.perform_tests()

        logging.debug("Write to %s", output)
        data_set.write_output(output, indent=indent)


class OptimalInterpolation(AbstractTask):
    """Creates a horizontal OI analysis of selected variables.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the OptimalInterpolation task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME")

    def execute(self):
        """Execute."""
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
        else:
            raise Exception(f"No translation for {self.var_name}")

        hlength = 30000
        vlength = 100000
        wlength = 0.5
        max_locations = 20
        elev_gradient = 0
        epsilon = 0.25

        uname = self.var_name.upper()
        hlength = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#HLENGTH", default=hlength)
        vlength = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#VLENGTH", default=vlength)
        wlength = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#WLENGTH", default=wlength)
        elev_gradient = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#GRADIENT", default=elev_gradient)
        max_locations = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#MAX_LOCATIONS",
                                                default=max_locations)
        epsilon = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#EPSILON", default=epsilon)
        minvalue = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#MINVALUE", default=None, abort=False)
        maxvalue = self.config.get_setting(f"OBSERVATIONS#OI#{uname}#MAXVALUE", default=None, abort=False)
        input_file = self.archive + "/raw_" + var + ".nc"
        output_file = self.archive + "/an_" + var + ".nc"

        # Get input fields
        geo, validtime, background, glafs, gelevs = \
            surfex.read_first_guess_netcdf_file(input_file, var)

        an_time = validtime
        # Read OK observations
        obs_file = self.exp_file_paths.get_system_file("obs_dir", "qc_" + var + ".json",
                                                       basedtg=self.dtg,
                                                       default_dir="default_obs_dir")
        observations = surfex.dataset_from_file(an_time, obs_file, qc_flag=0)

        field = surfex.horizontal_oi(geo, background, observations, gelevs=gelevs,
                                     hlength=hlength, vlength=vlength, wlength=wlength,
                                     max_locations=max_locations, elev_gradient=elev_gradient,
                                     epsilon=epsilon, minvalue=minvalue, maxvalue=maxvalue)

        logging.debug("Write output file %s", output_file)
        if os.path.exists(output_file):
            os.unlink(output_file)
        surfex.write_analysis_netcdf_file(output_file, field, var, validtime, gelevs, glafs,
                                          new_file=True, geo=geo)


class FirstGuess(AbstractTask):
    """Find first guess.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct a FistGuess task.

        Args:
            config (_type_): _description_
        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME", default=None)

    def execute(self):
        """Execute."""
        firstguess = self.config.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        logging.debug("DTG: %s BASEDTG: %s", self.dtg, self.fg_dtg)
        fg_file = self.exp_file_paths.get_system_file("first_guess_dir", firstguess,
                                                      basedtg=self.fg_dtg,
                                                      validtime=self.dtg,
                                                      default_dir="default_first_guess_dir")

        logging.info("Use first guess: %s", fg_file)
        if os.path.islink(self.fg_guess_sfx) or os.path.exists(self.fg_guess_sfx):
            os.unlink(self.fg_guess_sfx)
        os.symlink(fg_file, self.fg_guess_sfx)


class CycleFirstGuess(FirstGuess):
    """Cycle the first guess.

    Args:
        FirstGuess (_type_): _description_
    """

    def __init__(self, config):
        """Construct the cycled first guess object.

        Args:
            config (_type_): _description_
        """
        FirstGuess.__init__(self, config)

    def execute(self):
        """Execute."""
        firstguess = self.config.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        fg_file = self.exp_file_paths.get_system_file("first_guess_dir", firstguess,
                                                      basedtg=self.fg_dtg,
                                                      validtime=self.dtg,
                                                      default_dir="default_first_guess_dir")

        if os.path.islink(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(fg_file, self.fc_start_sfx)


class Oi2soda(AbstractTask):
    """Convert OI analysis to an ASCII file for SODA.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the Oi2soda task.

        Args:
            config (_type_): _description_
        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME")

    def execute(self):
        """Execute."""
        yy2 = self.dtg.strftime("%y")
        mm2 = self.dtg.strftime("%m")
        dd2 = self.dtg.strftime("%d")
        hh2 = self.dtg.strftime("%H")
        obfile = "OBSERVATIONS_" + yy2 + mm2 + dd2 + "H" + hh2 + ".DAT"
        output = self.config.exp_file_paths.get_system_file("obs_dir", obfile, basedtg=self.dtg,
                                                            default_dir="default_obs_dir")

        t2m = None
        rh2m = None
        s_d = None

        an_variables = {"t2m": False, "rh2m": False, "sd": False}
        obs_types = self.obs_types
        logging.debug("NNCO: %s", self.nnco)
        for ivar, __ in enumerate(obs_types):
            logging.debug("ivar=%s NNCO[ivar]=%s obtype=%s", ivar, self.nnco[ivar], obs_types[ivar])
            if self.nnco[ivar] == 1:
                if obs_types[ivar] == "T2M" or obs_types[ivar] == "T2M_P":
                    an_variables.update({"t2m": True})
                elif obs_types[ivar] == "HU2M" or obs_types[ivar] == "HU2M_P":
                    an_variables.update({"rh2m": True})
                elif obs_types[ivar] == "SWE":
                    an_variables.update({"sd": True})

        for var, var_name in an_variables.items():
            if an_variables[var]:
                var_name = self.translation[var]
                if var == "t2m":
                    t2m = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name
                    }
                elif var == "rh2m":
                    rh2m = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name
                    }
                elif var == "sd":
                    s_d = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name
                    }
        logging.debug("t2m  %s ", t2m)
        logging.debug("rh2m %s", rh2m)
        logging.debug("sd   %s", s_d)
        logging.debug("Write to %s", output)
        surfex.oi2soda(self.dtg, t2m=t2m, rh2m=rh2m, s_d=s_d, output=output)


class Qc2obsmon(AbstractTask):
    """Convert QC data to obsmon SQLite data.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the QC2obsmon data."""
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME")

    def execute(self):
        """Execute."""
        outdir = self.extrarch + "/ecma_sfc/" + self.dtg.strftime("%Y%m%d%H") + "/"
        os.makedirs(outdir, exist_ok=True)
        output = outdir + "/ecma.db"

        logging.debug("Write to %s", output)
        if os.path.exists(output):
            os.unlink(output)
        obs_types = self.obs_types
        for ivar, val in enumerate(self.nnco):
            if val == 1:
                if len(obs_types) > ivar:
                    if obs_types[ivar] == "T2M" or obs_types[ivar] == "T2M_P":
                        var_in = "t2m"
                    elif obs_types[ivar] == "HU2M" or obs_types[ivar] == "HU2M_P":
                        var_in = "rh2m"
                    elif obs_types[ivar] == "SWE":
                        var_in = "sd"
                    else:
                        raise NotImplementedError(obs_types[ivar])

                    if var_in != "sd":
                        var_name = self.translation[var_in]
                        q_c = self.obsdir + "/qc_" + var_name + ".json"
                        fg_file = self.archive + "/raw_" + var_name + ".nc"
                        an_file = self.archive + "/an_" + var_name + ".nc"
                        surfex.write_obsmon_sqlite_file(dtg=self.dtg, output=output, qc=q_c,
                                                        fg_file=fg_file,
                                                        an_file=an_file, varname=var_in,
                                                        file_var=var_name)


class FirstGuess4OI(AbstractTask):
    """Create a first guess to be used for OI.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the FirstGuess4OI task."""
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME")

    def execute(self):
        """Execute."""
        validtime = self.dtg

        extra = ""
        symlink_files = {}
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
            variables = [var]
            extra = "_" + var
            symlink_files.update({self.archive + "/raw.nc": "raw" + extra + ".nc"})
        else:
            var_in = []
            obs_types = self.obs_types
            for ivar, val in enumerate(self.nnco):
                if val == 1:
                    if len(obs_types) > ivar:
                        if obs_types[ivar] == "T2M" or obs_types[ivar] == "T2M_P":
                            var_in.append("t2m")
                        elif obs_types[ivar] == "HU2M" or obs_types[ivar] == "HU2M_P":
                            var_in.append("rh2m")
                        elif obs_types[ivar] == "SWE":
                            var_in.append("sd")
                        else:
                            raise NotImplementedError(obs_types[ivar])

            variables = []
            try:
                for var in var_in:
                    var_name = self.translation[var]
                    variables.append(var_name)
                    symlink_files.update({self.archive + "/raw_" + var_name + ".nc": "raw.nc"})
            except ValueError as ex:
                raise Exception("Variables could not be translated") from ex

        variables = variables + ["altitude", "land_area_fraction"]

        output = self.archive + "/raw" + extra + ".nc"
        cache_time = 3600
        # if "cache_time" in kwargs:
        #     cache_time = kwargs["cache_time"]
        cache = surfex.cache.Cache(cache_time)
        # cache = None
        if os.path.exists(output):
            logging.info("Output already exists " + output)
        else:
            self.write_file(output, variables, self.geo, validtime, cache=cache)

        # Create symlinks
        for target, linkfile in symlink_files.items():
            if os.path.lexists(target):
                os.unlink(target)
            os.symlink(linkfile, target)

    def write_file(self, output, variables, geo, validtime, cache=None):
        """Write the first guess file.

        Args:
            output (_type_): _description_
            variables (_type_): _description_
            geo (_type_): _description_
            validtime (_type_): _description_
            cache (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_

        """
        f_g = None
        for var in variables:
            identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
            inputfile = self.config.get_setting(identifier + "INPUTFILE", basedtg=self.fg_dtg,
                                                    validtime=self.dtg)
            if inputfile is None:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                inputfile = self.config.get_setting(identifier + "INPUTFILE", basedtg=self.fg_dtg,
                                                    validtime=self.dtg)
            identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
            fileformat = self.config.get_setting(identifier + "FILEFORMAT")
            if fileformat is None:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                fileformat = self.config.get_setting(identifier + "FILEFORMAT")

            identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
            converter = self.config.get_setting(identifier + "CONVERTER")
            if converter is None:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                converter = self.config.get_setting(identifier + "CONVERTER")

            identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
            input_geo_file = self.config.get_setting(identifier + "INPUT_GEO_FILE")
            if input_geo_file is None:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                input_geo_file = self.config.get_setting(identifier + "INPUT_GEO_FILE")

            logging.info("inputfile=%s, fileformat=%s", inputfile, fileformat)
            logging.info("converter=%s, input_geo_file=%s", converter, input_geo_file)
            # config_file = self.work_dir + "/config/first_guess.yml"
            config_file = self.config.first_guess_yml
            with open(config_file, mode="r", encoding="utf-8") as file_handler:
                config = yaml.safe_load(file_handler)
            logging.info("config_file=%s", config_file)
            defs = config[fileformat]
            geo_input = None
            if input_geo_file != "":
                geo_input = surfex.get_geo_object(open(input_geo_file, mode="r", encoding="utf-8"))
            defs.update({
                "filepattern": inputfile,
                "geo_input": geo_input
            })

            converter_conf = config[var][fileformat]["converter"]
            if converter not in config[var][fileformat]["converter"]:
                raise Exception(f"No converter {converter} definition found in {config_file}!")

            defs.update({"fcint": self.fcint})
            initial_basetime = validtime - timedelta(seconds=self.fgint)
            logging.debug("Converter=%s", str(converter))
            logging.debug("Converter_conf=%s", str(converter_conf))
            logging.debug("Defs=%s", defs)
            logging.debug("valitime=%s fcint=%s initial_basetime=%s", str(validtime),
                          str(self.fcint), str(initial_basetime))
            logging.debug("Fileformat: %s", fileformat)

            converter = surfex.read.Converter(converter, initial_basetime, defs, converter_conf,
                                              fileformat)
            field = surfex.read.ConvertedInput(geo, var, converter).read_time_step(validtime, cache)
            field = np.reshape(field, [geo.nlons, geo.nlats])

            # Create file
            if f_g is None:
                n_x = geo.nlons
                n_y = geo.nlats
                f_g = surfex.create_netcdf_first_guess_template(variables, n_x, n_y, output)
                f_g.variables["time"][:] = float(validtime.strftime("%s"))
                f_g.variables["longitude"][:] = np.transpose(geo.lons)
                f_g.variables["latitude"][:] = np.transpose(geo.lats)
                f_g.variables["x"][:] = [i for i in range(0, n_x)]
                f_g.variables["y"][:] = [i for i in range(0, n_y)]

            if var == "altitude":
                field[field < 0] = 0

            f_g.variables[var][:] = np.transpose(field)

        if f_g is not None:
            f_g.close()


class LogProgress(AbstractTask):
    """Log progress for restart.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the LogProgress task."""
        AbstractTask.__init__(self, config)

    def execute(self):

        progress = self.config.progress
        progress.update(dtg=self.next_dtg)
        progress.save_as_json(self.config.exp_dir, progress=True)


class LogProgressPP(AbstractTask):
    """Log progress for PP restart.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the LogProgressPP task."""
        AbstractTask.__init__(self, config)

    def execute(self):

        progress = self.config.progress
        progress.update(dtgpp=self.next_dtg)
        progress.save_as_json(self.config.exp_dir, progress_pp=True)
