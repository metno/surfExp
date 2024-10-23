"""General task module."""

import contextlib
import json
import os
import shutil

import numpy as np
import pysurfex
import yaml
from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import InterceptHandler, logger, logging
from deode.tasks.base import Task
from pysurfex.cache import Cache
from pysurfex.configuration import Configuration
from pysurfex.file import SurfFileTypeExtension
from pysurfex.geo import ConfProj, get_geo_object
from pysurfex.input_methods import get_datasources
from pysurfex.interpolation import horizontal_oi
from pysurfex.netcdf import (
    create_netcdf_first_guess_template,
    oi2soda,
    read_first_guess_netcdf_file,
    write_analysis_netcdf_file,
)
from pysurfex.obsmon import write_obsmon_sqlite_file
from pysurfex.pseudoobs import CryoclimObservationSet
from pysurfex.read import ConvertedInput, Converter
from pysurfex.run import BatchJob
from pysurfex.titan import TitanDataSet, dataset_from_file, define_quality_control

from surfexp.experiment import get_nnco


class PySurfexBaseTask(Task):
    """Base task class for pysurfex-experiment."""

    def __init__(self, config, name):
        """Construct pysurfex-experiment base class.

        Args:
        -------------------------------------------
            config (ParsedConfig): Configuration.
            name (str): Task name.

        """
        Task.__init__(self, config, name)
        logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

        # Domain/geo
        conf_proj = {
            "nam_conf_proj_grid": {
                "nimax": self.config["domain.nimax"],
                "njmax": self.config["domain.njmax"],
                "xloncen": self.config["domain.xloncen"],
                "xlatcen": self.config["domain.xlatcen"],
                "xdx": self.config["domain.xdx"],
                "xdy": self.config["domain.xdy"],
                "ilone": self.config["domain.ilone"],
                "ilate": self.config["domain.ilate"],
            },
            "nam_conf_proj": {
                "xlon0": self.config["domain.xlon0"],
                "xlat0": self.config["domain.xlat0"],
            },
        }

        self.geo = ConfProj(conf_proj)
        self.dtg = as_datetime(self.config["general.times.basetime"])
        self.fcint = as_timedelta("PT6H")

        self.translation = {
            "t2m": "air_temperature_2m",
            "rh2m": "relative_humidity_2m",
            "sd": "surface_snow_thickness",
        }
        self.obs_types = self.config["SURFEX.ASSIM.OBS.COBS_M"]
        self.nnco = get_nnco(self.config, basetime=self.dtg)

        self.fgint = as_timedelta(self.config["general.times.cycle_length"])
        self.fcint = as_timedelta(self.config["general.times.cycle_length"])
        self.fg_dtg = self.dtg - self.fgint
        self.next_dtg = self.dtg + self.fcint
        self.next_dtgpp = self.next_dtg

        self.fg_guess_sfx = self.wrk + "/first_guess_sfx"
        self.fc_start_sfx = self.wrk + "/fc_start_sfx"

        cfg = self.config["SURFEX"].dict()
        sfx_config = {"SURFEX": cfg}
        self.sfx_config = Configuration(sfx_config)
        update = {"SURFEX": {"ASSIM": {"OBS": {"NNCO": self.nnco}}}}
        self.config = self.config.copy(update=update)
        logger.debug("NNCO: {}", self.nnco)

    def substitute(self, pattern, basetime=None, validtime=None):
        fpattern = self.platform.substitute(
            pattern, basetime=basetime, validtime=validtime
        )
        if isinstance(fpattern, str):
            # @YYYY_FG@/@MM_FG@/@DD_FG@/@HH_FG@/
            if basetime is not None:
                fpattern = fpattern.replace("@YYYY_FG@", basetime.strftime("%Y"))
                fpattern = fpattern.replace("@MM_FG@", basetime.strftime("%m"))
                fpattern = fpattern.replace("@DD_FG@", basetime.strftime("%d"))
                fpattern = fpattern.replace("@HH_FG@", basetime.strftime("%H"))
        return fpattern

    def get_binary(self, binary):
        """Determine binary path from task or system config section.

        Args:
        -----------------------------------
            binary (str): Name of binary

        Returns:
        ---------------------------------------
            bindir (str): full path to binary

        """
        with contextlib.suppress(KeyError):
            binary = self.config[f"submission.task_exceptions.{self.name}.binary"]

        try:
            bindir = self.config[f"submission.task_exceptions.{self.name}.bindir"]
        except KeyError:
            bindir = self.platform.get_system_value("bindir")

        return f"{bindir}/{binary}"


class PrepareCycle(PySurfexBaseTask):
    """Prepare for th cycle to be run.

    Clean up existing directories.

    Args:
    -----------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct the PrepareCycle task.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "PrepareCycle")

    def run(self):
        """Override run."""
        self.execute()

    def execute(self):
        """Execute."""
        if os.path.exists(self.wrk):
            shutil.rmtree(self.wrk)


class QualityControl(PySurfexBaseTask):
    """Perform quality control of observations.

    Args:
    -------------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Constuct the QualityControl task.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "QualityControl")
        try:
            self.var_name = self.config["task.args.var_name"]
        except KeyError:
            self.var_name = None

    def execute(self):
        """Execute."""
        an_time = self.dtg

        obsdir = self.platform.get_system_value("obs_dir")
        os.makedirs(obsdir, exist_ok=True)

        fg_file = f"{self.platform.get_system_value('archive_dir')}/raw.nc"
        fg_file = self.platform.substitute(fg_file, basetime=self.dtg)

        # Default
        domain_file = f"{self.wdir}/domain.json"
        with open(domain_file, mode="w", encoding="utf8") as fh:
            json.dump(self.geo.json, fh)
        settings = {
            "domain": {"domain_file": domain_file},
            "firstguess": {
                "fg_file": fg_file,
                "fg_var": self.translation[self.var_name],
            },
        }
        default_tests = {
            "nometa": {"do_test": True},
            "domain": {
                "do_test": True,
            },
            "blacklist": {"do_test": True},
            "redundancy": {"do_test": True},
        }

        # T2M
        if self.var_name == "t2m":
            synop_obs = self.config["observations.synop_obs_t2m"]
            data_sets = {}
            if synop_obs:
                filepattern = self.config["observations.filepattern"]
                bufr_tests = default_tests
                bufr_tests.update(
                    {"plausibility": {"do_test": True, "maxval": 340, "minval": 200}}
                )

                data_sets.update(
                    {
                        "bufr": {
                            "filepattern": filepattern,
                            "filetype": "bufr",
                            "varname": ["airTemperatureAt2M"],
                            "tests": bufr_tests,
                        }
                    }
                )
            netatmo_obs = self.config["observations.netatmo_obs_t2m"]
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update(
                    {
                        "sct": {"do_test": True},
                        "plausibility": {"do_test": True, "maxval": 340, "minval": 200},
                    }
                )
                filepattern = self.config["observations.netatmo_filepattern"]
                data_sets.update(
                    {
                        "netatmo": {
                            "filepattern": filepattern,
                            "varname": "Temperature",
                            "filetype": "netatmo",
                            "tests": netatmo_tests,
                        }
                    }
                )

            settings.update({"sets": data_sets})

        # RH2M
        elif self.var_name == "rh2m":
            synop_obs = self.config["observations.synop_obs_rh2m"]
            data_sets = {}
            if synop_obs:
                filepattern = self.config["observations.filepattern"]
                bufr_tests = default_tests
                bufr_tests.update(
                    {"plausibility": {"do_test": True, "maxval": 100, "minval": 0}}
                )
                data_sets.update(
                    {
                        "bufr": {
                            "filepattern": filepattern,
                            "filetype": "bufr",
                            "varname": ["relativeHumidityAt2M"],
                            "tests": bufr_tests,
                        }
                    }
                )

            netatmo_obs = self.config["observations.netatmo_obs_rh2m"]
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update(
                    {
                        "sct": {"do_test": True},
                        "plausibility": {"do_test": True, "maxval": 10000, "minval": 0},
                    }
                )
                filepattern = self.config["observations.netatmo_filepattern"]
                data_sets.update(
                    {
                        "netatmo": {
                            "filepattern": filepattern,
                            "varname": "Humidity",
                            "filetype": "netatmo",
                            "tests": netatmo_tests,
                        }
                    }
                )

            settings.update({"sets": data_sets})

        # Snow Depth
        elif self.var_name == "sd":
            synop_obs = self.config["observations.synop_obs_sd"]
            cryo_obs = self.config["observations.cryo_obs_sd"]
            data_sets = {}
            if synop_obs:
                filepattern = self.config["observations.filepattern"]
                bufr_tests = default_tests
                bufr_tests.update(
                    {
                        "plausibility": {"do_test": True, "maxval": 1000, "minval": 0},
                        "firstguess": {"do_test": True, "negdiff": 0.5, "posdiff": 0.5},
                    }
                )
                data_sets.update(
                    {
                        "bufr": {
                            "filepattern": filepattern,
                            "filetype": "bufr",
                            "varname": ["totalSnowDepth"],
                            "tests": bufr_tests,
                        }
                    }
                )

            if cryo_obs:
                cryo_tests = default_tests
                cryo_tests.update(
                    {
                        "plausibility": {"do_test": True, "maxval": 1000, "minval": 0},
                        "firstguess": {"do_test": True, "negdiff": 0.5, "posdiff": 0.5},
                    }
                )
                filepattern = obsdir + "/cryo.json"
                data_sets.update(
                    {
                        "cryo": {
                            "filepattern": filepattern,
                            "filetype": "json",
                            "varname": "totalSnowDepth",
                            "tests": cryo_tests,
                        }
                    }
                )
            settings.update({"sets": data_sets})
        else:
            raise NotImplementedError

        logger.debug("Settings {}", json.dumps(settings, indent=2, sort_keys=True))

        output = obsdir + "/qc_" + self.translation[self.var_name] + ".json"
        lname = self.var_name.lower()

        try:
            tests = self.config[f"observations.qc.{lname}.tests"]
            logger.info("Using observations.qc.{lname}.tests")
        except KeyError:
            logger.info("Using default test observations.qc.tests")
            tests = self.config["observations.qc.tests"]

        indent = 2
        blacklist = {}
        with open("settings.json", mode="w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2)
        tests = define_quality_control(
            tests, settings, an_time, domain_geo=self.geo, blacklist=blacklist
        )

        datasources = get_datasources(an_time, settings["sets"])
        data_set = TitanDataSet(self.var_name, settings, tests, datasources, an_time)
        data_set.perform_tests()

        logger.debug("Write to {}", output)
        data_set.write_output(output, indent=indent)


class OptimalInterpolation(PySurfexBaseTask):
    """Creates a horizontal OI analysis of selected variables.

    Args:
    -----------------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct the OptimalInterpolation task.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "OptimalInterpolation")
        self.var_name = self.config["task.args.var_name"]

    def execute(self):
        """Execute."""
        archive = self.platform.get_system_value("archive_dir")
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
        else:
            raise KeyError(f"No translation for {self.var_name}")

        lname = self.var_name.lower()
        try:
            hlength = self.config[f"observations.oi.{lname}.hlength"]
        except KeyError:
            hlength = 30000

        try:
            vlength = self.config[f"observations.oi.{lname}.vlength"]
        except KeyError:
            vlength = 100000

        try:
            wlength = self.config[f"observations.oi.{lname}.wlength"]
        except KeyError:
            wlength = 0.5

        try:
            elev_gradient = self.config[f"observations.oi.{lname}.gradient"]
        except KeyError:
            elev_gradient = 0

        try:
            max_locations = self.config[f"observations.oi.{lname}.max_locations"]
        except KeyError:
            max_locations = 20

        try:
            epsilon = self.config[f"observations.oi.{lname}.epsilon"]
        except KeyError:
            epsilon = 0.25

        try:
            only_diff = self.config[f"observations.oi.{lname}.only_diff"]
        except KeyError:
            only_diff = False

        try:
            minvalue = self.config[f"observations.oi.{lname}.minvalue"]
        except KeyError:
            minvalue = None
        try:
            maxvalue = self.config[f"observations.oi.{lname}.maxvalue"]
        except KeyError:
            maxvalue = None
        input_file = archive + "/raw_" + var + ".nc"
        output_file = archive + "/an_" + var + ".nc"

        # Get input fields
        geo, validtime, background, glafs, gelevs = read_first_guess_netcdf_file(
            input_file, var
        )

        an_time = validtime
        # TODO
        an_time = an_time.replace(tzinfo=None)
        # Read OK observations
        obs_file = f"{self.platform.get_system_value('obs_dir')}/qc_{var}.json"
        logger.info("Obs file: {}", obs_file)
        observations = dataset_from_file(an_time, obs_file, qc_flag=0)
        field = horizontal_oi(
            geo,
            background,
            observations,
            gelevs=gelevs,
            hlength=hlength,
            vlength=vlength,
            wlength=wlength,
            max_locations=max_locations,
            elev_gradient=elev_gradient,
            epsilon=epsilon,
            minvalue=minvalue,
            maxvalue=maxvalue,
            interpol="bilinear",
            only_diff=only_diff,
        )
        logger.info("Write output file {}", output_file)
        if os.path.exists(output_file):
            os.unlink(output_file)
        write_analysis_netcdf_file(
            output_file, field, var, validtime, gelevs, glafs, new_file=True, geo=geo
        )


class FirstGuess(PySurfexBaseTask):
    """Find first guess.

    Args:
    -------------------------------
        Task (Task): Base class

    """

    def __init__(self, config, name=None):
        """Construct a FistGuess task.

        Args:
        -------------------------------------------------------
            config (ParsedObject): Parsed configuration
            name (str, optional): Task name. Defaults to None

        """
        if name is None:
            name = "FirstGuess"
        PySurfexBaseTask.__init__(self, config, name)
        try:
            self.var_name = self.config["task.var_name"]
        except KeyError:
            self.var_name = None
        # TODO
        masterodb = False
        try:
            lfagmap = self.sfx_config.get_setting("SURFEX#IO#LFAGMAP")
        except AttributeError:
            lfagmap = False
        self.csurf_filetype = self.sfx_config.get_setting("SURFEX#IO#CSURF_FILETYPE")
        self.suffix = SurfFileTypeExtension(
            self.csurf_filetype, lfagmap=lfagmap, masterodb=masterodb
        ).suffix
        self.fgint = as_timedelta(self.config["general.times.cycle_length"])
        self.fcint = as_timedelta(self.config["general.times.cycle_length"])
        self.fg_dtg = self.dtg - self.fgint

    def execute(self):
        """Execute."""
        firstguess = self.sfx_config.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        logger.debug("DTG: {} BASEDTG: {}", self.dtg, self.fg_dtg)
        fg_dir = self.config["system.archive_dir"]
        fg_dir = self.platform.substitute(
            fg_dir, basetime=self.fg_dtg, validtime=self.dtg
        )
        fg_file = f"{fg_dir}/{firstguess}"

        logger.info("Use first guess: {}", fg_file)
        if os.path.islink(self.fg_guess_sfx) or os.path.exists(self.fg_guess_sfx):
            os.unlink(self.fg_guess_sfx)
        os.symlink(fg_file, self.fg_guess_sfx)


class CryoClim2json(PySurfexBaseTask):
    """Find first guess.

    Args:
    -----------------------------------
        Task (Task): Base class

    """

    def __init__(self, config, name=None):
        """Construct a FistGuess task.

        Args:
        ---------------------------------------------------------
            config (ParsedObject): Parsed configuration
            name (str, optional): Task name. Defaults to None

        """
        if name is None:
            name = "CryoClim2json"
        PySurfexBaseTask.__init__(self, config, name)
        try:
            self.var_name = self.config["task.var_name"]
        except KeyError:
            self.var_name = None

    def execute(self):
        """Execute."""
        archive = self.platform.get_system_value("archive_dir")
        var = "surface_snow_thickness"
        input_file = archive + "/raw_" + var + ".nc"

        # Get input fields
        geo, validtime, background, glafs, gelevs = read_first_guess_netcdf_file(
            input_file, var
        )

        obs_file = self.config["observations.cryo_filepattern"]
        obs_file = [self.platform.substitute(obs_file)]
        try:
            laf_threshold = self.config["observations.cryo_laf_threshold"]
        except AttributeError:
            laf_threshold = 0.1
        try:
            step = self.config["observations.cryo_step"]
        except AttributeError:
            step = 2
        try:
            fg_threshold = self.config["observations.cryo_fg_threshold"]
        except AttributeError:
            fg_threshold = 0.4
        try:
            new_snow_depth = self.config["observations.cryo_new_snow"]
        except AttributeError:
            new_snow_depth = 0.1
        try:
            cryo_varname = self.config["observations.cryo_varname"]
        except AttributeError:
            cryo_varname = None
        obs_set = CryoclimObservationSet(
            [obs_file],
            validtime,
            geo,
            background,
            gelevs,
            step=step,
            fg_threshold=fg_threshold,
            new_snow_depth=new_snow_depth,
            glaf=glafs,
            laf_threshold=laf_threshold,
            cryo_varname=cryo_varname,
        )
        obs_set.write_json_file(f"{self.platform.get_system_value('obs_dir')}/cryo.json")


class CycleFirstGuess(FirstGuess):
    """Cycle the first guess.

    Args:
    ------------------------------------------
        FirstGuess (FirstGuess): Base class

    """

    def __init__(self, config):
        """Construct the cycled first guess object.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        FirstGuess.__init__(self, config, "CycleFirstGuess")

    def execute(self):
        """Execute."""
        firstguess = self.sfx_config.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        fg_dir = self.config["system.archive_dir"]
        fg_dir = self.platform.substitute(
            fg_dir, basetime=self.fg_dtg, validtime=self.dtg
        )
        fg_file = f"{fg_dir}/{firstguess}"

        if os.path.islink(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(fg_file, self.fc_start_sfx)


class Oi2soda(PySurfexBaseTask):
    """Convert OI analysis to an ASCII file for SODA.

    Args:
    --------------------------------------
        Task (AbstractClass): Base class

    """

    def __init__(self, config):
        """Construct the Oi2soda task.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "Oi2soda")
        try:
            self.var_name = self.config["task.args.var_name"]
        except KeyError:
            self.var_name = None

    def execute(self):
        """Execute."""
        yy2 = self.dtg.strftime("%y")
        mm2 = self.dtg.strftime("%m")
        dd2 = self.dtg.strftime("%d")
        hh2 = self.dtg.strftime("%H")
        obfile = "OBSERVATIONS_" + yy2 + mm2 + dd2 + "H" + hh2 + ".DAT"
        output = f"{self.platform.get_system_value('obs_dir')}/{obfile}"

        t2m = None
        rh2m = None
        s_d = None

        archive = self.platform.get_system_value("archive_dir")
        an_variables = {"t2m": False, "rh2m": False, "sd": False}
        obs_types = self.obs_types
        logger.debug("NNCO: {}", self.nnco)
        for ivar, __ in enumerate(obs_types):
            logger.info(
                "ivar={} NNCO[ivar]={} obtype={}",
                ivar,
                self.nnco[ivar],
                obs_types[ivar],
            )
            if self.nnco[ivar] == 1:
                if obs_types[ivar] == "T2M" or obs_types[ivar] == "T2M_P":
                    an_variables.update({"t2m": True})
                elif obs_types[ivar] == "HU2M" or obs_types[ivar] == "HU2M_P":
                    an_variables.update({"rh2m": True})
                elif obs_types[ivar] == "SWE":
                    an_variables.update({"sd": True})

        logger.info(an_variables)
        for var, status in an_variables.items():
            if status:
                lvar_name = self.translation[var]
                if var == "t2m":
                    t2m = {
                        "file": archive + "/an_" + lvar_name + ".nc",
                        "var": lvar_name,
                    }
                elif var == "rh2m":
                    rh2m = {
                        "file": archive + "/an_" + lvar_name + ".nc",
                        "var": lvar_name,
                    }
                elif var == "sd":
                    s_d = {
                        "file": archive + "/an_" + lvar_name + ".nc",
                        "var": lvar_name,
                    }
        logger.info("t2m  {} ", t2m)
        logger.info("rh2m {}", rh2m)
        logger.info("sd   {}", s_d)
        logger.debug("Write to {}", output)
        oi2soda(self.dtg, t2m=t2m, rh2m=rh2m, s_d=s_d, output=output)


class Qc2obsmon(PySurfexBaseTask):
    """Convert QC data to obsmon SQLite data.

    Args:
    ---------------------------------------
        Task (AbstractClass): Base class

    """

    def __init__(self, config):
        """Construct the QC2obsmon data.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "Qc2obsmon")
        try:
            self.var_name = self.config["task.args.var_name"]
        except KeyError:
            self.var_name = None

    def execute(self):
        """Execute."""
        archive = self.platform.get_system_value("archive_dir")
        extrarch = self.platform.get_system_value("extrarch_dir")
        obsdir = self.platform.get_system_value("obs_dir")
        outdir = extrarch + "/ecma_sfc/" + self.dtg.strftime("%Y%m%d%H") + "/"
        os.makedirs(outdir, exist_ok=True)
        output = outdir + "/ecma.db"

        logger.debug("Write to {}", output)
        if os.path.exists(output):
            os.unlink(output)
        obs_types = self.obs_types
        for ivar, val in enumerate(self.nnco):
            if val == 1 and len(obs_types) > ivar:
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
                    q_c = obsdir + "/qc_" + var_name + ".json"
                    fg_file = archive + "/raw_" + var_name + ".nc"
                    an_file = archive + "/an_" + var_name + ".nc"
                    write_obsmon_sqlite_file(
                        dtg=self.dtg,
                        output=output,
                        qc=q_c,
                        fg_file=fg_file,
                        an_file=an_file,
                        varname=var_in,
                        file_var=var_name,
                    )


class FirstGuess4OI(PySurfexBaseTask):
    """Create a first guess to be used for OI.

    Args:
    -------------------------------------
        Task (AbstractClass): Base class

    """

    def __init__(self, config):
        """Construct the FirstGuess4OI task.

        Args:
        -------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "FirstGuess4OI")
        try:
            self.var_name = self.config["task.var_name"]
        except KeyError:
            self.var_name = None

    def execute(self):
        """Execute."""
        validtime = self.dtg

        extra = ""
        symlink_files = {}
        archive = self.platform.get_system_value("archive_dir")
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
            variables = [var]
            extra = "_" + var
            symlink_files.update({archive + "/raw.nc": "raw" + extra + ".nc"})
        else:
            var_in = []
            obs_types = self.obs_types
            for ivar, val in enumerate(self.nnco):
                if val == 1 and len(obs_types) > ivar:
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
                    symlink_files.update({archive + "/raw_" + var_name + ".nc": "raw.nc"})
            except KeyError as exc:
                raise KeyError("Variables could not be translated") from exc

        variables = [*variables, "altitude", "land_area_fraction"]

        output = archive + "/raw" + extra + ".nc"
        cache_time = 3600
        cache = Cache(cache_time)
        if os.path.exists(output):
            logger.info("Output already exists {}", output)
        else:
            os.makedirs(os.path.dirname(output), exist_ok=True)
            self.write_file(output, variables, self.geo, validtime, cache=cache)

        # Create symlinks
        for target, linkfile in symlink_files.items():
            if os.path.lexists(target):
                os.unlink(target)
            os.symlink(linkfile, target)

    def write_file(self, output, variables, geo, validtime, cache=None):
        """Write the first guess file.

        Args:
        --------------------------------------------------------
            output (str): Output file
            variables (list): Variables
            geo (Geo): Geometry
            validtime (as_datetime): Validtime
            cache (Cache, optional): Cache. Defaults to None.

        Raises:
        ---------------------------------------------
            KeyError: Converter not found
            RuntimeError: No valid data read

        """
        f_g = None
        for var in variables:
            lvar = var.lower()
            try:
                identifier = "initial_conditions.fg4oi." + lvar + "."
                inputfile = self.config[identifier + "inputfile"]
            except KeyError:
                identifier = "initial_conditions.fg4oi."
                inputfile = self.config[identifier + "inputfile"]

            logger.info("inputfile0={}", inputfile)
            inputfile = self.substitute(
                inputfile, basetime=self.fg_dtg, validtime=self.dtg
            )
            logger.info("inputfile1={}", inputfile)

            try:
                identifier = "initial_conditions.fg4oi." + lvar + "."
                fileformat = self.config[identifier + "fileformat"]
            except KeyError:
                identifier = "initial_conditions.fg4oi."
                fileformat = self.config[identifier + "fileformat"]
            fileformat = self.platform.substitute(
                fileformat, basetime=self.fg_dtg, validtime=self.dtg
            )

            try:
                identifier = "initial_conditions.fg4oi." + lvar + "."
                converter = self.config[identifier + "converter"]
            except KeyError:
                identifier = "initial_conditions.fg4oi."
                converter = self.config[identifier + "converter"]

            try:
                identifier = "initial_conditions.fg4oi." + lvar + "."
                input_geo_file = self.config[identifier + "input_geo_file"]
            except KeyError:
                identifier = "initial_conditions.fg4oi."
                input_geo_file = self.config[identifier + "input_geo_file"]

            logger.info("inputfile={}, fileformat={}", inputfile, fileformat)
            logger.info("converter={}, input_geo_file={}", converter, input_geo_file)

            try:
                config_file = self.config["pysurfex.first_guess_yml_file"]
            except KeyError:
                config_file = None
            if config_file is None or config_file == "":
                config_file = f"{os.path.dirname(pysurfex.__path__[0])}/pysurfex/cfg/first_guess.yml"
            with open(config_file, mode="r", encoding="utf-8") as file_handler:
                config = yaml.safe_load(file_handler)
            logger.info("config_file={}", config_file)
            defs = config[fileformat]
            geo_input = None
            if input_geo_file != "":
                with open(input_geo_file, mode="r", encoding="utf-8") as fh:
                    geo_dict = json.load(fh)
                geo_input = get_geo_object(geo_dict)
            defs.update({"filepattern": inputfile, "geo_input": geo_input})

            converter_conf = config[var][fileformat]["converter"]
            if converter not in config[var][fileformat]["converter"]:
                raise KeyError(
                    f"No converter {converter} definition found in {config_file}!"
                )

            defs.update({"fcint": self.fcint.total_seconds()})
            initial_basetime = validtime - self.fgint
            logger.debug("Converter={}", str(converter))
            logger.debug("Converter_conf={}", str(converter_conf))
            logger.debug("Defs={}", defs)
            logger.debug(
                "valitime={} fcint={} initial_basetime={}",
                str(validtime),
                str(self.fcint),
                str(initial_basetime),
            )
            logger.debug("Fileformat: {}", fileformat)

            logger.info(
                "Set up converter. defs={} converter_conf={}", defs, converter_conf
            )
            converter = Converter(
                converter, initial_basetime, defs, converter_conf, fileformat
            )
            logger.info("Read converted input for var={} validtime={}", var, validtime)
            field = ConvertedInput(geo, var, converter).read_time_step(validtime, cache)
            field = np.reshape(field, [geo.nlons, geo.nlats])

            if np.all(np.isnan(field)):
                raise RuntimeError("All data read are undefined!")

            # Create file
            if f_g is None:
                n_x = geo.nlons
                n_y = geo.nlats
                f_g = create_netcdf_first_guess_template(variables, n_x, n_y, output)
                f_g.variables["time"][:] = float(validtime.strftime("%s"))
                f_g.variables["longitude"][:] = np.transpose(geo.lons)
                f_g.variables["latitude"][:] = np.transpose(geo.lats)
                f_g.variables["x"][:] = [range(n_x)]
                f_g.variables["y"][:] = [range(n_y)]

            if var == "altitude":
                field[field < 0] = 0

            f_g.variables[var][:] = np.transpose(field)

        if f_g is not None:
            f_g.close()


class LogProgress(PySurfexBaseTask):
    """Log progress for restart.

    Args:
    ------------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct the LogProgress task.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "LogProgress")

    def execute(self):
        """Execute."""


class LogProgressPP(PySurfexBaseTask):
    """Log progress for PP restart.

    Args:
    -------------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct the LogProgressPP task.

        Args:
        ---------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "LogProgressPP")

    def execute(self):
        """Execute."""


class FetchMarsObs(PySurfexBaseTask):
    """Fetch observations from Mars.

    Args:
    -----------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct the FetchMarsObs task.

        Args:
        ---------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "FetchMarsObs")
        self.basetime = as_datetime(self.config["basetime"])
        self.obsdir = self.platform.get_system_value("obs_dir")

    def execute(self):
        """Execute."""
        basetime_str = self.basetime.strftime("%Y%m%d%H")
        date_str = self.basetime.strftime("%Y%m%d")
        obfile = f"{self.obsdir}/ob{basetime_str}"
        request_file = "mars.req"
        side_window = as_timedelta("PT90M")
        window = side_window + side_window - as_timedelta("PT1M")
        window = str(int(window.total_seconds()) / 60)
        start_time = (self.basetime - side_window).strftime("%H%M")
        with open(request_file, mode="w", encoding="utf-8") as fhandler:
            fhandler.write("RETRIEVE,\n")
            fhandler.write("REPRES   = BUFR,\n")
            fhandler.write("TYPE     = OB,\n")
            fhandler.write(f"TIME     = {start_time},\n")
            fhandler.write(f"RANGE    = {window},\n")
            fhandler.write("AREA     = 090/-180/041/180,")
            fhandler.write("OBSTYPE  = LSD/SSD/SLNS/VSNS,\n")
            fhandler.write(f"DATE     = {date_str},\n")
            fhandler.write(f"TARGET   = '{obfile}'\n")

        cmd = f"mars {request_file}"
        try:
            batch = BatchJob(os.environ)
            logger.info("Running {}", cmd)
            batch.run(cmd)
        except RuntimeError as exc:
            raise RuntimeError from exc
