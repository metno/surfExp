"""General task module."""

import contextlib
import json
import os
import shutil

from deode.datetime_utils import as_datetime, as_timedelta, get_decade
from deode.logs import InterceptHandler
from deode.logs import builtin_logging as logging
from deode.logs import logger
from deode.os_utils import deodemakedirs
from deode.tasks.base import Task
from pysurfex.cli import (
    cli_oi2soda,
    cryoclim_pseudoobs,
    first_guess_for_oi,
    gridpp,
    qc2obsmon,
    titan,
)
from pysurfex.geo import ConfProj
from pysurfex.platform_deps import SystemFilePaths
from pysurfex.run import BatchJob
from pysurfex.verification import converter2harp_cli

from surfexp.experiment import SettingsFromNamelistAndConfig


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
        self.climdir = self.platform.get_system_value("climdir")
        deodemakedirs(self.climdir)
        domain_json = self.geo.json
        domain_json.update({"nam_pgd_grid": {"cgrid": "CONF PROJ"}})
        self.domain_file = f"{self.climdir}/domain.json"
        if not os.path.exists(self.domain_file):
            with open(self.domain_file, mode="w", encoding="utf-8") as file_handler:
                json.dump(domain_json, file_handler, indent=2)
        # self.dtg = as_datetime(self.config["general.times.basetime"])
        self.basetime = as_datetime(self.config["general.times.basetime"])
        casedir = self.config["system.casedir"]
        self.casedir = self.platform.substitute(casedir, basetime=self.basetime)
        self.archive = self.platform.get_system_value("archive")
        self.archive_path = self.config["system.archive_dir"]

        self.translation = {
            "t2m": "air_temperature_2m",
            "rh2m": "relative_humidity_2m",
            "sd": "surface_snow_thickness",
        }

        # Namelist settings
        self.soda_settings = SettingsFromNamelistAndConfig("soda", config)
        self.suffix = (
            f'.{self.soda_settings.get_setting("NAM_IO_OFFLINE#CSURF_FILETYPE").lower()}'
        )
        self.obs_types = self.soda_settings.get_setting("NAM_OBS#COBS_M", default=[])
        self.nnco = self.soda_settings.get_nnco(self.config, basetime=self.basetime)
        logger.debug("NNCO: {}", self.nnco)

        self.fgint = as_timedelta(self.config["general.times.cycle_length"])
        self.fcint = as_timedelta(self.config["general.times.cycle_length"])
        self.fg_basetime = self.basetime - self.fgint
        self.next_dtg = self.basetime + self.fcint
        self.next_dtgpp = self.next_dtg

        # Binary input data
        self.input_definition = self.platform.get_system_value("sfx_input_definition")
        # Create PySurfex system paths
        system_paths = self.config["system"].dict()
        platform_paths = self.config["platform"].dict()
        exp_file_paths = {}
        for key, val in system_paths.items():
            lkey = self.platform.substitute(key)
            lval = self.platform.substitute(val)
            exp_file_paths.update({lkey: lval})
        for key, val in platform_paths.items():
            lkey = self.platform.substitute(key)
            lval = self.platform.substitute(val)
            exp_file_paths.update({lkey: lval})
        self.exp_file_paths = SystemFilePaths(exp_file_paths)

    def get_exp_file_paths_file(self):
        exp_file_paths_file = "exp_file_paths.json"
        self.exp_file_paths.save_as(exp_file_paths_file)
        return exp_file_paths_file

    def substitute(self, pattern, basetime=None, micro="@"):
        logger.debug("pattern in {}", pattern)
        fpattern = pattern
        for key, val in self.exp_file_paths.system_file_paths.items():
            logger.debug("key: {} val={}", key, val)
            if isinstance(key, str) and isinstance(val, str):
                fpattern = fpattern.replace(f"{micro}{key.upper()}{micro}", val)
                fpattern = fpattern.replace(f"{micro}{key.lower()}{micro}", val)

        if isinstance(fpattern, str):
            if basetime is not None:
                decade_key = "decade"
                if self.config["pgd.one_decade"]:
                    decade_val = get_decade(basetime)
                else:
                    decade_val = ""
                logger.debug("decade key={} val={}", decade_key, decade_val)
                fpattern = fpattern.replace(
                    f"{micro}{decade_key.upper()}{micro}", decade_val
                )
                fpattern = fpattern.replace(
                    f"{micro}{decade_key.lower()}{micro}", decade_val
                )
        logger.debug("pattern out {}", fpattern)
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
            bindir = self.config["submission.bindir"]
        except KeyError:
            bindir = None
        try:
            bindir = self.config[f"submission.task_exceptions.{self.name}.bindir"]
        except KeyError:
            pass

        # surfExp binary directory
        bindir_system = self.platform.get_system_value("bindir")

        bin_paths = [f"{bindir_system}/{binary}-offline", f"{bindir_system}/{binary}"]
        for bin_path in bin_paths:
            if os.path.exists(bin_path):
                return bin_path

        bin_path = f"{bindir}/{binary}"
        try:
            if os.path.exists(bin_path):
                return bin_path
        except FileNotFoundError:
            raise RuntimeError() from FileNotFoundError

    def get_first_guess(self, basetime):
        csurffile = self.soda_settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")
        firstguess = f"{csurffile}{self.suffix}"

        fcint = as_timedelta(self.config["general.times.cycle_length"])
        fg_basetime = basetime - fcint
        logger.debug("DTG: {} BASEDTG: {}", basetime, fg_basetime)
        fg_dir = self.config["system.archive_dir"]
        fg_dir = self.platform.substitute(
            fg_dir, basetime=fg_basetime, validtime=basetime
        )
        fg_file = f"{fg_dir}/{firstguess}"

        logger.info("Use first guess: {}", fg_file)
        return fg_file

    def get_forecast_start_file(self, basetime, mode):
        csurffile = self.soda_settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")
        archive = self.config["system.archive_dir"]
        if self.config["an_forcing.enabled"]:
            archive = self.platform.substitute(archive, basetime=basetime)
        else:
            archive = self.platform.substitute(archive, basetime=(basetime-self.fcint))
        csurffile = f"{archive}/{csurffile}{self.suffix}"
        analysis = f"{archive}/ANALYSIS{self.suffix}"

        logger.info("do_prep={}", self.config["suite_control.do_prep"])
        logger.info("basetime={}", basetime)
        logger.info("starttime={}", as_datetime(self.config["general.times.start"]))
        logger.info("archive={}", archive)
        if self.config["suite_control.do_prep"] and basetime == as_datetime(
            self.config["general.times.start"]
        ):
            cprepfile = self.soda_settings.get_setting(
                "NAM_IO_OFFLINE#CPREPFILE", default="PREP"
            )
            prep_file = f"{archive}/{cprepfile}{self.suffix}"
            if os.path.exists(prep_file):
                logger.info("Found PREP file {}", prep_file)
                return prep_file
            else:
                raise FileNotFoundError(prep_file)
        for fname in [analysis, csurffile]:
            if os.path.exists(fname):
                logger.info("Using {} as initial conditions", fname)
                return fname
            logger.warning("Could not find possible initial condition {}", fname)
        raise RuntimeError("No initial conditions found")


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
            raise RuntimeError from KeyError
        try:
            self.offset = int(self.config["task.args.offset"])
        except KeyError:
            self.offset = 0
        self.validtime = self.basetime - as_timedelta(f"{self.offset:02d}:00:00")

    def execute(self):
        """Execute."""
        logger.info("Analysis time: {}", self.validtime)

        obsdir = self.config["system.obs_dir"]
        obsdir = self.platform.substitute(obsdir, basetime=self.validtime)

        os.makedirs(obsdir, exist_ok=True)

        archive = self.config["system.archive_dir"]
        archive = self.platform.substitute(archive, basetime=self.validtime)
        fg_file = f"{archive}/raw.nc"

        # Default
        settings = {
            "domain": {"domain_file": self.domain_file},
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
                filepattern = self.platform.substitute(
                    filepattern, basetime=self.validtime
                )
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
                filepattern = self.platform.substitute(
                    filepattern, basetime=self.validtime
                )
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
                filepattern = self.platform.substitute(
                    filepattern, basetime=self.validtime
                )
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
                filepattern = self.platform.substitute(
                    filepattern, basetime=self.validtime
                )
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
                filepattern = self.platform.substitute(
                    filepattern, basetime=self.validtime
                )
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
            logger.info("Using observations.qc.%s.tests", lname)
        except KeyError:
            logger.info("Using default test observations.qc.tests")
            tests = self.config["observations.qc.tests"]

        try:
            indent = self.config["observations.qc.indent"]
        except KeyError:
            indent = 2

        try:
            blacklist_file = self.config[f"observations.qc.{lname}.blacklist"]
            logger.info("Using variable specific blacklist file {}", blacklist_file)
        except KeyError:
            blacklist_file = None
        if blacklist_file is None:
            try:
                blacklist_file = self.config["observations.qc.blacklist"]
                logger.info("Using general blacklist file {}", blacklist_file)
            except KeyError:
                blacklist_file = None

        settings_file = "settings.json"
        with open(settings_file, mode="w", encoding="utf-8") as fh:
            json.dump({self.var_name: settings}, fh, indent=2)

        argv = [
            "-i",
            settings_file,
            "-v",
            self.var_name,
            "-o",
            output,
            "--indent",
            str(indent),
            "--validtime",
            self.basetime.strftime("%Y%m%d%H"),
            "--domain",
            self.domain_file,
        ]
        if blacklist_file is not None:
            argv += ["--blacklist", blacklist_file]

        tests = list(tests)
        argv += tests
        titan(argv)


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
        try:
            self.var_name = self.config["task.args.var_name"]
        except KeyError:
            raise RuntimeError from KeyError
        try:
            self.offset = int(self.config["task.args.offset"])
        except KeyError:
            self.offset = 0
        self.validtime = self.basetime - as_timedelta(f"{self.offset:02d}:00:00")

    def execute(self):
        """Execute."""
        archive = self.config["system.archive_dir"]
        archive = self.platform.substitute(archive, basetime=self.validtime)
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

        logger.info("Analysis time: {}", self.validtime)
        # Read OK observations
        obs_dir = self.config["system.obs_dir"]
        obs_dir = self.platform.substitute(obs_dir, basetime=self.validtime)
        obs_file = f"{obs_dir}/qc_{var}.json"

        argv = [
            "-i",
            input_file,
            "-obs",
            obs_file,
            "-o",
            output_file,
            "-v",
            var,
            "-hor",
            str(hlength),
            "-vert",
            str(vlength),
            "--wlength",
            str(wlength),
            "--maxLocations",
            str(max_locations),
            "--elevGradient",
            str(elev_gradient),
            "--epsilon",
            str(epsilon),
        ]
        if maxvalue is not None:
            argv += ["--maxvalue", maxvalue]
        if minvalue is not None:
            argv += ["--minvalue", minvalue]
        if only_diff:
            argv += ["--only_diff"]
        if os.path.exists(output_file):
            os.unlink(output_file)
        gridpp(argv)


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
        fg_input_file = archive + "/raw_" + var + ".nc"

        obs_file = self.config["observations.cryo_filepattern"]
        # obs_file = [self.platform.substitute(obs_file)]
        try:
            laf_threshold = self.config["observations.cryo_laf_threshold"]
        except AttributeError:
            laf_threshold = 1.0
        try:
            step = self.config["observations.cryo_step"]
        except KeyError:
            step = 2
        try:
            cryo_slope_file = self.config["observations.cryo_slope_file"]
        except KeyError:
            raise RuntimeError("Missing cryo slope file") from KeyError
        try:
            cryo_perm_snow_file = self.config["observations.cryo_perm_snow_file"]
        except KeyError:
            raise RuntimeError("Missing cryo perm snow file") from KeyError
        try:
            cryo_varname = self.config["observations.cryo_varname"]
        except KeyError:
            cryo_varname = None

        output = f"{self.platform.get_system_value('obs_dir')}/cryo.json"
        argv = [
            "--infiles",
            obs_file,
            "--laf_treshold",
            str(laf_threshold),
            "-step",
            str(step),
            "-o",
            output,
        ]
        if cryo_varname is not None:
            argv += ["-iv", cryo_varname]

        # First guess
        argv += ["fg", "--inputfile", fg_input_file, "-v", var]

        # Slope
        argv += [
            "slope",
            "--inputfile",
            cryo_slope_file,
            "-v",
            "SFX.SSO_SLOPE",
            "-it",
            "surfex",
        ]
        # Permanent snow
        argv += [
            "perm_snow",
            "--inputfile",
            cryo_perm_snow_file,
            "-v",
            "SFX.COVER006",
            "-it",
            "surfex",
        ]
        cryoclim_pseudoobs(argv)


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
        yy2 = self.basetime.strftime("%y")
        mm2 = self.basetime.strftime("%m")
        dd2 = self.basetime.strftime("%d")
        hh2 = self.basetime.strftime("%H")
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

        argv = ["-o", output, self.basetime.strftime("%Y%m%d%H")]
        logger.info(an_variables)
        for var, status in an_variables.items():
            if status:
                lvar_name = self.translation[var]
                if var == "t2m":
                    t2m = {
                        "file": archive + "/an_" + lvar_name + ".nc",
                        "var": lvar_name,
                    }
                    argv += [
                        "--t2m_file",
                        archive + "/an_" + lvar_name + ".nc",
                        "--t2m_var",
                        lvar_name,
                    ]

                elif var == "rh2m":
                    rh2m = {
                        "file": archive + "/an_" + lvar_name + ".nc",
                        "var": lvar_name,
                    }
                    argv += [
                        "--rh2m_file",
                        archive + "/an_" + lvar_name + ".nc",
                        "--rh2m_var",
                        lvar_name,
                    ]
                elif var == "sd":
                    s_d = {
                        "file": archive + "/an_" + lvar_name + ".nc",
                        "var": lvar_name,
                    }
                    argv += [
                        "--sd_file",
                        archive + "/an_" + lvar_name + ".nc",
                        "--sd_var",
                        lvar_name,
                    ]
        logger.info("t2m  {} ", t2m)
        logger.info("rh2m {}", rh2m)
        logger.info("sd   {}", s_d)
        logger.debug("Write to {}", output)
        cli_oi2soda(argv)


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
        try:
            self.offset = int(self.config["task.args.offset"])
        except KeyError:
            self.offset = 0
        try:
            self.mode = self.config["task.args.mode"]
        except KeyError:
            raise RuntimeError("Mode not set") from KeyError
        self.validtime = self.basetime - as_timedelta(f"{self.offset:02d}:00:00")

    def execute(self):
        """Execute."""
        logger.info("Analysis time: {}", self.validtime)
        archive = self.platform.get_system_value("archive_dir")
        extrarch = self.platform.get_system_value("extrarch_dir")
        obsdir = self.platform.get_system_value("obs_dir")
        outdir = f"{extrarch}/ecma_sfc/{self.validtime.strftime('%Y%m%d%H')}/{self.mode}"
        os.makedirs(outdir, exist_ok=True)
        output = outdir + "/ecma.db"

        logger.info("Write to {}", output)
        if os.path.exists(output):
            os.unlink(output)

        variables = []
        if self.mode == "an_forcing":
            # Add variables if used in analysed forcing
            try:
                an_forcing = self.config["an_forcing"]["enabled"]
            except KeyError:
                raise RuntimeError("an_forcing should not be false")
            try:
                an_forc_vars = self.config["an_forcing"]["variables"]
            except KeyError:
                an_forc_vars = []
            if an_forcing:
                for var in an_forc_vars:
                    if var not in variables:
                        variables.append(var)
        else:
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
                    variables.append(var_in)

        for var_in in variables:
            var_name = self.translation[var_in]
            q_c = obsdir + "/qc_" + var_name + ".json"
            fg_file = archive + "/raw_" + var_name + ".nc"
            an_file = archive + "/an_" + var_name + ".nc"
            argv = [
                "--operator",
                "bilinear",
                "--file_var",
                var_name,
                "--an_file",
                an_file,
                "--fg_file",
                fg_file,
                "-o",
                output,
            ]
            argv += [self.basetime.strftime("%Y%m%d%H"), var_name, q_c]
            qc2obsmon(argv)


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
        try:
            self.offset = int(self.config["task.args.offset"])
        except KeyError:
            self.offset = 0
        try:
            self.mode = self.config["task.args.mode"]
        except KeyError:
            raise RuntimeError from KeyError

        self.validtime = self.basetime - as_timedelta(f"{self.offset:02d}:00:00")
        #self.basetime = self.basetime - self.fcint
        logger.info("basetime: {}", self.basetime)
        logger.info("validtime: {}", self.validtime)

    def execute(self):
        """Execute."""

        extra = ""
        symlink_files = {}
        archive = self.config["system.archive_dir"]
        archive = self.platform.substitute(archive, basetime=self.validtime)
        deodemakedirs(archive)

        if self.var_name in self.translation:
            var = self.translation[self.var_name]
            variables = [var]
            extra = "_" + var
            symlink_files.update({archive + "/raw.nc": "raw" + extra + ".nc"})
        else:
            var_in = []
            if self.mode == "analysis":
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
            if self.mode == "an_forcing":
                try:
                    an_forc_vars = self.config["an_forcing.variables"]
                except KeyError:
                    an_forc_vars = []

                for var in an_forc_vars:
                    if var not in var_in:
                        var_in.append(var)

            variables = []
            raw_vars = []
            try:
                for var in var_in:
                    var_name = self.translation[var]
                    variables.append(var_name)
                    raw_vars.append(var)
                    symlink_files.update({archive + "/raw_" + var_name + ".nc": "raw.nc"})
            except KeyError as exc:
                raise KeyError("Variables could not be translated") from exc

        variables = [*variables, "altitude", "land_area_fraction"]
        raw_vars = [*raw_vars, "altitude", "laf"]

        output = archive + "/raw" + extra + ".nc"

        argv = ["--fg-variables"]
        for var in raw_vars:
            print(var)
            argv.append(var)
        argv += [
            "--validtime",
            self.validtime.strftime("%Y%m%d%H"),
            "--domain",
            self.domain_file,
            "-o",
            output,
            "--debug",
        ]

        for __, var in enumerate(raw_vars):
            argv += [f"--{var}-system-file-paths", self.get_exp_file_paths_file()]
            settings = self.config[f"initial_conditions.fg4oi.{self.mode}.{var}"]
            for setting, val in settings.items():
                if isinstance(val, str):
                    prev_basetime = self.basetime - self.fcint
                    val = val.replace("@BASETIME@", self.basetime.strftime("%Y%m%d%H"))
                    val = val.replace("@FG_BASETIME@", prev_basetime.strftime("%Y%m%d%H"))
                    val = val.replace("@VALIDTIME@", self.validtime.strftime("%Y%m%d%H"))
                    val = val.replace("@DECADE@", get_decade(prev_basetime))
                    val = [val]
                val = list(val)
                argv += [f"--{var}-{setting}"] + val

        logger.info("argv: {}", " ".join(argv))
        first_guess_for_oi(argv)

        # Create symlinks
        for target, linkfile in symlink_files.items():
            if os.path.lexists(target):
                os.unlink(target)
            os.symlink(linkfile, target)

    def get_var_settings(self, var):
        lvar = var.lower()
        try:
            identifier = f"initial_conditions.fg4oi.{self.mode}.{lvar}.inputfile"
            inputfile = self.config[identifier]
        except KeyError:
            identifier = f"initial_conditions.fg4oi.{self.mode}.inputfile"
            inputfile = self.config[identifier]
        logger.info("inputfile0={} basetime={}", inputfile, self.basetime)
        inputfile = self.substitute(inputfile, basetime=self.basetime)
        logger.info("inputfile1={}", inputfile)

        try:
            identifier = f"initial_conditions.fg4oi.{self.mode}.{lvar}.fileformat"
            fileformat = self.config[identifier]
        except KeyError:
            identifier = f"initial_conditions.fg4oi.{self.mode}.fileformat"
            fileformat = self.config[identifier]

        try:
            identifier = f"initial_conditions.fg4oi.{self.mode}.{lvar}.converter"
            converter = self.config[identifier]
        except KeyError:
            identifier = f"initial_conditions.fg4oi.{self.mode}.converter"
            converter = self.config[identifier]

        try:
            identifier = f"initial_conditions.fg4oi.{self.mode}.{lvar}.geo_input_file"
            geo_input_file = self.config[identifier]
        except KeyError:
            try:
                identifier = f"initial_conditions.fg4oi.{self.mode}.geo_input_file"
                geo_input_file = self.config[identifier]
            except KeyError:
                geo_input_file = ""
        try:
            identifier = f"initial_conditions.fg4oi.{self.mode}.{lvar}.config"
            config = self.config[identifier]
        except KeyError:
            try:
                identifier = f"initial_conditions.fg4oi.{self.mode}.config"
                config = self.config[identifier]
            except KeyError:
                config = ""

        logger.info("inputfile={}, fileformat={}", inputfile, fileformat)
        logger.info("converter={}, geo_input_file={}", converter, geo_input_file)
        logger.info("config={}", config)
        return inputfile, fileformat, converter, geo_input_file, config


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


class HarpSQLite(PySurfexBaseTask):
    """Extract observations to SQLite for HARP.

    Args:
    -----------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct the HarpSQLite task.

        Args:
        ---------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "HarpSQLite")

        try:
            self.var_name = self.config["task.args.var_name"]
        except KeyError:
            raise RuntimeError("Var name is needed") from KeyError
        try:
            self.basetime = as_datetime(self.config["task.args.basetime"])
        except KeyError:
            raise RuntimeError("Basetime is needed") from KeyError
        try:
            self.validtime = as_datetime(self.config["task.args.validtime"])
        except KeyError:
            raise RuntimeError("Validtime is needed") from KeyError
        try:
            mode = self.config["task.args.mode"]
        except KeyError:
            raise RuntimeError("Mode is needed") from KeyError
        try:
            self.harp_param = self.config[
                f"verification.{mode}.{self.var_name}.harp_param"
            ]
        except KeyError:
            raise RuntimeError("harp param is needed") from KeyError
        try:
            self.harp_param_unit = self.config[
                f"verification.{mode}.{self.var_name}.harp_param_unit"
            ]
        except KeyError:
            raise RuntimeError("harp_param_unit is needed") from KeyError

        self.model = self.platform.substitute(
            self.config["extractsqlite.sqlite_model_name"]
        )
        self.stationlist_file = self.platform.substitute(
            self.config["extractsqlite.station_list"]
        )
        self.sqlite_path = self.platform.substitute(
            self.config["extractsqlite.sqlite_path"]
        )
        self.sqlite_template = self.platform.substitute(
            self.config["extractsqlite.sqlite_template"]
        )

        archive = self.config["system.archive_dir"]
        archive = self.platform.substitute(
            archive, basetime=self.basetime, validtime=self.validtime
        )
        if mode == "forecast":
            archive = f"{archive}/forecast/"
        input_pattern = f"{archive}/SURFOUT.@YYYY_LL@@MM_LL@@DD_LL@_@HH_LL@h00.nc"
        logger.info("validtime={}", self.validtime)
        logger.info("archive={}", archive)
        self.input = self.substitute(input_pattern)  # , basetime=self.basetime)
        logger.info("input={}", self.input)
        # self.input = "/lustre/storeB/users/trygveasp//deode/CY49DT_OFFLINE_DRAMMEN/archive/@FG_YYYY@/@FG_MM@/@FG_DD@/@FG_HH@/SURFOUT.@YYYY_LL@@MM_LL@@DD_LL@_@HH_LL@h00.nc"

    def execute(self):
        """Execute."""

        dt_string = self.validtime.strftime("%Y%m%d%H")
        basetime = self.basetime.strftime("%Y%m%d%H")
        print(basetime)
        argv = [
            "--station-list",
            self.stationlist_file,
            "-b",
            basetime,
            "--harp-param",
            self.harp_param,
            "--harp-param-unit",
            self.harp_param_unit,
            "--model-name",
            self.model,
            "-o",
            f"{self.sqlite_path}/{self.sqlite_template}",
            "converter",
            "-i",
            self.input,
            "-it",
            "surfex",
            "-v",
            self.var_name,
            "-t",
            dt_string,
            "-b",
            basetime,
        ]
        logger.info("Args: {}", " ".join(argv))
        converter2harp_cli(argv=argv)


class StartOfflineSfx(PySurfexBaseTask):
    """Start offline surfex suite from control suite.

    Args:
    -----------------------------------
        Task (_type_): _description_

    """

    def __init__(self, config):
        PySurfexBaseTask.__init__(self, config, "StartOfflineSfx")
        try:
            self.run_cmd = self.config["task.args.run_cmd"]
        except KeyError:
            raise RuntimeError from KeyError

    def execute(self):
        """Execute."""
        logger.info("Running command: {}", self.run_cmd)
        try:
            BatchJob(os.environ).run(self.run_cmd)
        except Exception as exc:
            raise RuntimeError("Command failed") from exc
