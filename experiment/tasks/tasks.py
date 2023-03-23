"""General task module."""
import json
import os
import shutil

import numpy as np
import surfex
import yaml

from ..config_parser import ParsedConfig
from ..configuration import Configuration
from ..datetime_utils import as_datetime, as_timedelta, datetime_as_string
from ..experiment import ExpFromConfig
from ..logs import get_logger_from_config
from ..toolbox import FileManager


class AbstractTask(object):
    """General abstract task to be implemented by all tasks using default container."""

    def __init__(self, config):
        """Initialize a task run by the default ecflow container.

        All tasks implelementing this base class will work with the default
        ecflow container

        Args:
            config (ParsedObject): Parsed configuration

        Raises:
            RuntimeError: Surfex not loaded

        """
        if surfex is None:
            raise RuntimeError("Surfex module not properly loaded!")

        self.config = config
        self.logger = get_logger_from_config(config)
        self.logger.debug("Create task")
        self.fmanager = FileManager(self.config)
        self.platform = self.fmanager.platform
        self.settings = Configuration(self.config)
        self.dtg = as_datetime(config.get_value("general.times.basetime"))
        self.basetime = as_datetime(config.get_value("general.times.basetime"))
        self.starttime = as_datetime(config.get_value("general.times.start"))
        self.dtgbeg = as_datetime(config.get_value("general.times.start"))

        self.host = "0"
        self.work_dir = self.platform.get_system_value("sfx_exp_data")
        self.lib = self.platform.get_system_value("sfx_exp_lib")
        try:
            self.stream = self.config.get_value("general.stream")
        except AttributeError:
            self.stream = None

        self.surfex_config = self.platform.get_system_value("surfex_config")
        self.sfx_exp_vars = None
        self.logger.debug(
            "   config: %s", json.dumps(config.dict(), sort_keys=True, indent=2)
        )

        mbr = self.config.get_value("general.realization")
        if isinstance(mbr, str) and mbr == "":
            mbr = None
        if mbr is not None:
            mbr = int(mbr)
        self.mbr = mbr
        self.members = self.config.get_value("general.realizations")

        # Domain/geo
        conf_proj = {
            "nam_conf_proj_grid": {
                "nimax": self.config.get_value("domain.nimax"),
                "njmax": self.config.get_value("domain.njmax"),
                "xloncen": self.config.get_value("domain.xloncen"),
                "xlatcen": self.config.get_value("domain.xlatcen"),
                "xdx": self.config.get_value("domain.xdx"),
                "xdy": self.config.get_value("domain.xdy"),
                "ilone": self.config.get_value("domain.ilone"),
                "ilate": self.config.get_value("domain.ilate"),
            },
            "nam_conf_proj": {
                "xlon0": self.config.get_value("domain.xlon0"),
                "xlat0": self.config.get_value("domain.xlat0"),
            },
        }

        self.geo = surfex.ConfProj(conf_proj)
        self.fmanager = FileManager(config)
        self.platform = self.fmanager.platform
        wrapper = self.config.get_value("task.wrapper")
        if wrapper is None:
            wrapper = ""
        self.wrapper = wrapper

        masterodb = False
        try:
            lfagmap = self.config.get_value("SURFEX.IO.LFAGMAP")
        except AttributeError:
            lfagmap = False
        self.csurf_filetype = self.config.get_value("SURFEX.IO.CSURF_FILETYPE")
        self.suffix = surfex.SurfFileTypeExtension(
            self.csurf_filetype, lfagmap=lfagmap, masterodb=masterodb
        ).suffix

        # TODO Move to config
        ###########################################################################
        wrk = self.config.get_value("system.wrk")
        self.wrk = self.platform.substitute(wrk)
        os.makedirs(self.wrk, exist_ok=True)
        archive = self.platform.get_system_value("archive_dir")
        self.archive = self.platform.substitute(archive)

        os.makedirs(self.archive, exist_ok=True)
        self.bindir = self.platform.get_system_value("bin_dir")

        self.extrarch = self.platform.get_system_value("extrarch_dir")

        os.makedirs(self.extrarch, exist_ok=True)
        self.obsdir = self.platform.get_system_value("obs_dir")
        os.makedirs(self.obsdir, exist_ok=True)

        # TODO
        self.fgint = as_timedelta(self.config.get_value("general.times.cycle_length"))
        self.fcint = as_timedelta(self.config.get_value("general.times.cycle_length"))
        self.fg_dtg = self.dtg - self.fgint
        self.next_dtg = self.dtg + self.fcint
        self.next_dtgpp = self.next_dtg

        first_guess_dir = self.platform.get_system_value("archive_dir")
        first_guess_dir = self.platform.substitute(first_guess_dir, basetime=self.fg_dtg)
        self.first_guess_dir = first_guess_dir
        self.input_path = self.platform.get_system_value("namelist_dir")
        ###########################################################################

        self.wdir = str(os.getpid())
        self.wdir = self.wrk + "/" + self.wdir
        self.logger.info("WDIR=%s", self.wdir)
        os.makedirs(self.wdir, exist_ok=True)
        os.chdir(self.wdir)

        self.fg_guess_sfx = self.wrk + "/first_guess_sfx"
        self.fc_start_sfx = self.wrk + "/fc_start_sfx"

        self.translation = {
            "t2m": "air_temperature_2m",
            "rh2m": "relative_humidity_2m",
            "sd": "surface_snow_thickness",
        }
        self.obs_types = self.config.get_value("SURFEX.ASSIM.OBS.COBS_M")

        self.nnco = self.settings.get_nnco(dtg=self.basetime)
        update = {"SURFEX": {"ASSIM": {"OBS": {"NNCO": self.nnco}}}}
        self.config = self.config.copy(update=update)
        self.logger.debug("NNCO: %s", self.nnco)

    def run(self):
        """Run task.

        Define run sequence.

        """
        self.execute()
        self.postfix()

    def execute(self):
        """Do nothing for base execute task."""
        self.logger.warning("Using empty base class execute")

    def postfix(self):
        """Do default postfix.

        Default is to clean.

        """
        self.logger.info("Base class postfix")
        if self.wrk is not None:
            os.chdir(self.wrk)

        if self.wdir is not None:
            shutil.rmtree(self.wdir)


class PrepareCycle(AbstractTask):
    """Prepare for th cycle to be run.

    Clean up existing directories.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the PrepareCycle task.

        Args:
            config (ParsedObject): Parsed configuration

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
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config)
        print(self.config.get_value("task"))
        self.var_name = self.config.get_value("task.var_name")
        print(self.var_name)

    def execute(self):
        """Execute."""
        an_time = self.dtg

        sfx_lib = self.platform.get_system_value("sfx_exp_lib")

        fg_file = f"{self.platform.get_system_value('archive_dir')}/raw.nc"
        fg_file = self.platform.substitute(fg_file, basetime=self.dtg)

        # Default
        settings = {
            "domain": {"domain_file": sfx_lib + "/domain.json"},
            "firstguess": {"fg_file": fg_file, "fg_var": self.translation[self.var_name]},
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
            synop_obs = self.config.get_value("observations.synop_obs_t2m")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update(
                    {"plausibility": {"do_test": True, "maxval": 340, "minval": 200}}
                )
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update(
                    {
                        "bufr": {
                            "filepattern": filepattern,
                            "filetype": "bufr",
                            "varname": "airTemperatureAt2M",
                            "tests": bufr_tests,
                        }
                    }
                )
            netatmo_obs = self.config.get_value("observations.netatmo_obs_t2m")
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update(
                    {
                        "sct": {"do_test": True},
                        "plausibility": {"do_test": True, "maxval": 340, "minval": 200},
                    }
                )
                filepattern = self.config.get_value("observations.netatmo_filepattern")
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
            synop_obs = self.config.get_value("observations.synop_obs_rh2m")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update(
                    {"plausibility": {"do_test": True, "maxval": 100, "minval": 0}}
                )
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update(
                    {
                        "bufr": {
                            "filepattern": filepattern,
                            "filetype": "bufr",
                            "varname": "relativeHumidityAt2M",
                            "tests": bufr_tests,
                        }
                    }
                )

            netatmo_obs = self.config.get_value("observations.netatmo_obs_rh2m")
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update(
                    {
                        "sct": {"do_test": True},
                        "plausibility": {"do_test": True, "maxval": 10000, "minval": 0},
                    }
                )
                filepattern = self.config.get_value("observations.netatmo_filepattern")
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
            synop_obs = self.config.get_value("observations.synop_obs_sd")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update(
                    {
                        "plausibility": {"do_test": True, "maxval": 1000, "minval": 0},
                        "firstguess": {"do_test": True, "negdiff": 0.5, "posdiff": 0.5},
                    }
                )
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update(
                    {
                        "bufr": {
                            "filepattern": filepattern,
                            "filetype": "bufr",
                            "varname": "totalSnowDepth",
                            "tests": bufr_tests,
                        }
                    }
                )

            settings.update({"sets": data_sets})
        else:
            raise NotImplementedError

        self.logger.debug("Settings %s", json.dumps(settings, indent=2, sort_keys=True))

        output = self.obsdir + "/qc_" + self.translation[self.var_name] + ".json"
        lname = self.var_name.lower()

        try:
            tests = self.config.get_value(f"observations.qc.{lname}.tests")
            self.logger.info("Using observations.qc.{lname}.tests")
        except AttributeError:
            self.logger.info("Using default test observations.qc.tests")
            tests = self.config.get_value("observations.qc.tests")

        indent = 2
        blacklist = {}
        json.dump(settings, open("settings.json", mode="w", encoding="utf-8"), indent=2)
        tests = surfex.titan.define_quality_control(
            tests, settings, an_time, domain_geo=self.geo, blacklist=blacklist
        )

        datasources = surfex.obs.get_datasources(an_time, settings["sets"])
        data_set = surfex.TitanDataSet(
            self.var_name, settings, tests, datasources, an_time
        )
        data_set.perform_tests()

        self.logger.debug("Write to %s", output)
        data_set.write_output(output, indent=indent)


class OptimalInterpolation(AbstractTask):
    """Creates a horizontal OI analysis of selected variables.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the OptimalInterpolation task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_value("task.var_name")

    def execute(self):
        """Execute."""
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
        else:
            raise KeyError(f"No translation for {self.var_name}")

        hlength = 30000
        vlength = 100000
        wlength = 0.5
        max_locations = 20
        elev_gradient = 0
        epsilon = 0.25

        lname = self.var_name.lower()
        hlength = self.config.get_value(
            f"observations.oi.{lname}.hlength", default=hlength
        )
        vlength = self.config.get_value(
            f"observations.oi.{lname}.vlength", default=vlength
        )
        wlength = self.config.get_value(
            f"observations.oi.{lname}.wlength", default=wlength
        )
        elev_gradient = self.config.get_value(
            f"observations.oi.{lname}.gradient", default=elev_gradient
        )
        max_locations = self.config.get_value(
            f"observations.oi.{lname}.max_locations", default=max_locations
        )
        epsilon = self.config.get_value(
            f"observations.oi.{lname}.epsilon", default=epsilon
        )
        try:
            minvalue = self.config.get_value(
                f"observations.oi.{lname}.minvalue", default=None
            )
        except AttributeError:
            minvalue = None
        try:
            maxvalue = self.config.get_value(
                f"observations.oi.{lname}.maxvalue", default=None
            )
        except AttributeError:
            maxvalue = None
        input_file = self.archive + "/raw_" + var + ".nc"
        output_file = self.archive + "/an_" + var + ".nc"

        # Get input fields
        geo, validtime, background, glafs, gelevs = surfex.read_first_guess_netcdf_file(
            input_file, var
        )

        an_time = validtime
        # Read OK observations
        obs_file = f"{self.platform.get_system_value('obs_dir')}/qc_{var}.json"
        observations = surfex.dataset_from_file(an_time, obs_file, qc_flag=0)

        field = surfex.horizontal_oi(
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
        )

        self.logger.debug("Write output file %s", output_file)
        if os.path.exists(output_file):
            os.unlink(output_file)
        surfex.write_analysis_netcdf_file(
            output_file, field, var, validtime, gelevs, glafs, new_file=True, geo=geo
        )


class FirstGuess(AbstractTask):
    """Find first guess.

    Args:
        AbstractTask (AbstractTask): Base class
    """

    def __init__(self, config):
        """Construct a FistGuess task.

        Args:
            config (ParsedObject): Parsed configuration
        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_value("task.var_name", default=None)

    def execute(self):
        """Execute."""
        firstguess = self.config.get_value("SURFEX.IO.CSURFFILE") + self.suffix
        self.logger.debug("DTG: %s BASEDTG: %s", self.dtg, self.fg_dtg)
        fg_dir = self.config.get_value("system.archive_dir")
        fg_dir = self.platform.substitute(
            fg_dir, basetime=self.fg_dtg, validtime=self.dtg
        )
        fg_file = f"{fg_dir}/{firstguess}"

        self.logger.info("Use first guess: %s", fg_file)
        if os.path.islink(self.fg_guess_sfx) or os.path.exists(self.fg_guess_sfx):
            os.unlink(self.fg_guess_sfx)
        os.symlink(fg_file, self.fg_guess_sfx)


class CycleFirstGuess(FirstGuess):
    """Cycle the first guess.

    Args:
        FirstGuess (FirstGuess): Base class
    """

    def __init__(self, config):
        """Construct the cycled first guess object.

        Args:
            config (ParsedObject): Parsed configuration

        """
        FirstGuess.__init__(self, config)

    def execute(self):
        """Execute."""
        firstguess = self.config.get_value("SURFEX.IO.CSURFFILE") + self.suffix
        fg_dir = self.config.get_value("system.archive_dir")
        fg_dir = self.platform.substitute(
            fg_dir, basetime=self.fg_dtg, validtime=self.dtg
        )
        fg_file = f"{fg_dir}/{firstguess}"

        if os.path.islink(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(fg_file, self.fc_start_sfx)


class Oi2soda(AbstractTask):
    """Convert OI analysis to an ASCII file for SODA.

    Args:
        AbstractTask (AbstractClass): Base class
    """

    def __init__(self, config):
        """Construct the Oi2soda task.

        Args:
            config (ParsedObject): Parsed configuration
        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_value("task.var_name")

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

        an_variables = {"t2m": False, "rh2m": False, "sd": False}
        obs_types = self.obs_types
        self.logger.debug("NNCO: %s", self.nnco)
        for ivar, __ in enumerate(obs_types):
            self.logger.debug(
                "ivar=%s NNCO[ivar]=%s obtype=%s", ivar, self.nnco[ivar], obs_types[ivar]
            )
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
                        "var": var_name,
                    }
                elif var == "rh2m":
                    rh2m = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name,
                    }
                elif var == "sd":
                    s_d = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name,
                    }
        self.logger.debug("t2m  %s ", t2m)
        self.logger.debug("rh2m %s", rh2m)
        self.logger.debug("sd   %s", s_d)
        self.logger.debug("Write to %s", output)
        surfex.oi2soda(self.dtg, t2m=t2m, rh2m=rh2m, s_d=s_d, output=output)


class Qc2obsmon(AbstractTask):
    """Convert QC data to obsmon SQLite data.

    Args:
        AbstractTask (AbstractClass): Base class
    """

    def __init__(self, config):
        """Construct the QC2obsmon data.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_value("task.var_name")

    def execute(self):
        """Execute."""
        outdir = self.extrarch + "/ecma_sfc/" + self.dtg.strftime("%Y%m%d%H") + "/"
        os.makedirs(outdir, exist_ok=True)
        output = outdir + "/ecma.db"

        self.logger.debug("Write to %s", output)
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
                        surfex.write_obsmon_sqlite_file(
                            dtg=self.dtg,
                            output=output,
                            qc=q_c,
                            fg_file=fg_file,
                            an_file=an_file,
                            varname=var_in,
                            file_var=var_name,
                        )


class FirstGuess4OI(AbstractTask):
    """Create a first guess to be used for OI.

    Args:
        AbstractTask (AbstractClass): Base class
    """

    def __init__(self, config):
        """Construct the FirstGuess4OI task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_value("task.var_name")

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
                    symlink_files.update(
                        {self.archive + "/raw_" + var_name + ".nc": "raw.nc"}
                    )
            except KeyError as exc:
                raise KeyError("Variables could not be translated") from exc

        variables = variables + ["altitude", "land_area_fraction"]

        output = self.archive + "/raw" + extra + ".nc"
        cache_time = 3600
        cache = surfex.cache.Cache(cache_time)
        if os.path.exists(output):
            self.logger.info("Output already exists %s", output)
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
            output (str): Output file
            variables (list): Variables
            geo (Geo): Geometry
            validtime (as_datetime): Validtime
            cache (Cache, optional): Cache. Defaults to None.

        Raises:
            KeyError: Converter not found

        """
        f_g = None
        for var in variables:
            lvar = var.lower()
            try:
                identifier = "initial_conditions.fg4oi." + lvar + "#"
                inputfile = self.config.get_value(identifier + "inputfile")
            except AttributeError:
                identifier = "initial_conditions.fg4oi."
                inputfile = self.config.get_value(identifier + "inputfile")

            print(inputfile)
            inputfile = self.platform.substitute(
                inputfile, basetime=self.fg_dtg, validtime=self.dtg
            )

            try:
                identifier = "initial_conditions.fg4oi." + lvar + "#"
                fileformat = self.config.get_value(identifier + "fileformat")
            except AttributeError:
                identifier = "initial_conditions.fg4oi."
                fileformat = self.config.get_value(identifier + "fileformat")
            fileformat = self.platform.substitute(
                fileformat, basetime=self.fg_dtg, validtime=self.dtg
            )

            try:
                identifier = "initial_conditions.fg4oi." + lvar + "#"
                converter = self.config.get_value(identifier + "converter")
            except AttributeError:
                identifier = "initial_conditions.fg4oi."
                converter = self.config.get_value(identifier + "converter")

            try:
                identifier = "initial_conditions.fg4oi." + lvar + "#"
                input_geo_file = self.config.get_value(identifier + "input_geo_file")
            except AttributeError:
                identifier = "initial_conditions.fg4oi."
                input_geo_file = self.config.get_value(identifier + "input_geo_file")

            self.logger.info("inputfile=%s, fileformat=%s", inputfile, fileformat)
            self.logger.info("converter=%s, input_geo_file=%s", converter, input_geo_file)

            config_file = self.platform.get_system_value("first_guess_yml")
            with open(config_file, mode="r", encoding="utf-8") as file_handler:
                config = yaml.safe_load(file_handler)
            self.logger.info("config_file=%s", config_file)
            defs = config[fileformat]
            geo_input = None
            if input_geo_file != "":
                geo_input = surfex.get_geo_object(
                    open(input_geo_file, mode="r", encoding="utf-8")
                )
            defs.update({"filepattern": inputfile, "geo_input": geo_input})

            converter_conf = config[var][fileformat]["converter"]
            if converter not in config[var][fileformat]["converter"]:
                raise KeyError(
                    f"No converter {converter} definition found in {config_file}!"
                )

            defs.update({"fcint": self.fcint.total_seconds()})
            initial_basetime = validtime - self.fgint
            self.logger.debug("Converter=%s", str(converter))
            self.logger.debug("Converter_conf=%s", str(converter_conf))
            self.logger.debug("Defs=%s", defs)
            self.logger.debug(
                "valitime=%s fcint=%s initial_basetime=%s",
                str(validtime),
                str(self.fcint),
                str(initial_basetime),
            )
            self.logger.debug("Fileformat: %s", fileformat)

            converter = surfex.read.Converter(
                converter, initial_basetime, defs, converter_conf, fileformat
            )
            field = surfex.read.ConvertedInput(geo, var, converter).read_time_step(
                validtime, cache
            )
            field = np.reshape(field, [geo.nlons, geo.nlats])

            # Create file
            if f_g is None:
                n_x = geo.nlons
                n_y = geo.nlats
                f_g = surfex.create_netcdf_first_guess_template(
                    variables, n_x, n_y, output
                )
                f_g.variables["time"][:] = float(validtime.strftime("%s"))
                f_g.variables["longitude"][:] = np.transpose(geo.lons)
                f_g.variables["latitude"][:] = np.transpose(geo.lats)
                f_g.variables["x"][:] = [range(0, n_x)]
                f_g.variables["y"][:] = [range(0, n_y)]

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
        """Construct the LogProgress task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""
        progress = {
            "basetime": datetime_as_string(self.next_dtg),
            "validtime": datetime_as_string(self.next_dtg),
        }
        loglevel = self.config.get_value("general.loglevel")

        config_file = self.config.get_value("metadata.source_file_path")
        config = ParsedConfig.from_file(config_file)
        sfx_exp = ExpFromConfig(config.dict(), progress, loglevel=loglevel)
        sfx_exp.dump_json(config_file, indent=2)


class LogProgressPP(AbstractTask):
    """Log progress for PP restart.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct the LogProgressPP task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""
        progress = {"basetime_pp": datetime_as_string(self.next_dtg)}
        loglevel = self.config.get_value("general.loglevel")

        config_file = self.config.get_value("metadata.source_file_path")
        config = ParsedConfig.from_file(config_file)
        sfx_exp = ExpFromConfig(config.dict(), progress, loglevel=loglevel)
        sfx_exp.dump_json(config_file, indent=2)
