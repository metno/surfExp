"""Tasks running surfex binaries."""
import json

from deode.datetime_utils import as_datetime, as_timedelta, get_decade
from deode.logs import logger
from deode.namelist import NamelistGenerator
from deode.os_utils import deodemakedirs
from pysurfex.cli import offline, perturbed_offline, pgd, prep, soda

from surfexp.experiment import SettingsFromNamelistAndConfig, check_consistency
from surfexp.tasks.tasks import PySurfexBaseTask


class SurfexBinaryTask(PySurfexBaseTask):
    """Task."""

    def __init__(self, config, name):
        """Construct object.

        Args:
            config (deode.ParsedConfig): Configuration
            name (str): Name

        """
        PySurfexBaseTask.__init__(self, config, name)
        self.one_decade = self.config["pgd.one_decade"]
        check_consistency(self.config)

    def get_pgdfile(self, basetime):
        """Get path to PGD file. Take decade into account.

        Args:
            basetime (as_datetime): Basetime

        Returns:
            str: Path to PGD fil
        """
        decade = f"_{get_decade(as_datetime(basetime))}" if self.one_decade else ""
        pgdfile = f"{self.climdir}/PGD{decade}{self.suffix}"
        return pgdfile

    def execute(self):
        """Execute."""
        raise NotImplementedError


class OfflinePgd(SurfexBinaryTask):
    """Task."""

    def __init__(self, config):
        """Construct object.

        Args:
            config (deode.ParsedConfig): Configuration

        """
        SurfexBinaryTask.__init__(self, config, __class__.__name__)
        self.nlgen = NamelistGenerator(self.config, "surfex")
        self.one_decade = self.config["pgd.one_decade"]
        self.basetime = as_datetime(config["task.args.basetime"])
        self.mode = "pgd"
        try:
            self.args = self.config[f"{self.mode}.args"]
        except KeyError:
            self.args = {}
        try:
            self.wrapper = f"{self.config['submission.task.wrapper']}"
        except KeyError:
            self.wrapper = ""

    def execute(self):
        """Execute."""
        # Create namelists
        nml_file = "OPTIONS_input.nam"
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        settings.nam_gen.write(nml_file)

        output = f"{self.get_pgdfile(self.basetime)}"
        binary = self.get_binary("PGD")

        try:
            wrapper = self.config["submission.task.wrapper"]
        except KeyError:
            wrapper = ""
        # PGD arguments
        argv = [
            "--domain",
            self.domain_file,
            "--system-file-paths",
            self.get_exp_file_paths_file(),
            "--basetime",
            self.basetime.strftime("%Y%m%d%H"),
            "--namelist-path",
            nml_file,
            "--input-binary-data",
            self.input_definition,
            "--binary",
            binary,
            "--wrapper",
            wrapper,
            "--output",
            output,
        ]
        if self.one_decade:
            argv += ["--one-decade"]

        for key, val in self.args.items():
            if f"--{key}" not in argv:
                if isinstance(val, bool):
                    if val:
                        argv += [f"--{key}"]
                else:
                    argv += [f"--{key}", val]
            else:
                logger.warning("setting {} can not be overriden", key)

        # Run PGD
        logger.info("argv={}", argv)
        pgd(argv=argv)
        self.archive_logs(["OPTIONS.nam", "LISTING_PGD.txt"], target=self.climdir)


class OfflinePrep(SurfexBinaryTask):
    """Prep."""

    def __init__(self, config):
        """Construct object.

        Args:
            config (deode.ParsedConfig): Configuration

        """
        SurfexBinaryTask.__init__(self, config, __class__.__name__)
        self.nlgen = NamelistGenerator(self.config, "surfex")
        self.mode = "prep"
        try:
            self.args = self.config[f"{self.mode}.args"]
        except KeyError:
            self.args = {}
        try:
            self.wrapper = f"{self.config['submission.task.wrapper']}"
        except KeyError:
            self.wrapper = ""

    def execute(self):
        """Execute."""
        cnmexp = self.config["general.cnmexp"]
        output = f"{self.archive}/ICMSH{cnmexp}INIT.sfx"

        binary = self.get_binary("PREP")
        deodemakedirs(self.archive)

        nml_file = "OPTIONS_input.nam"
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        settings.nam_gen.write(nml_file)

        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"

        cprepfile = self.soda_settings.get_setting("NAM_IO_OFFLINE#CPREPFILE")
        cprepfile = f"{cprepfile}{self.suffix}"

        archive = self.platform.get_system_value("archive_dir")
        output = (
            f"{self.platform.substitute(archive, basetime=self.basetime)}/{cprepfile}"
        )

        # PREP arguments output
        argv = [
            "--system-file-paths",
            self.get_exp_file_paths_file(),
            "--pgd",
            pgd_file_path,
            "--basetime",
            self.basetime.strftime("%Y%m%d%H"),
            "--namelist-path",
            nml_file,
            "--input-binary-data",
            self.input_definition,
            "--output",
            output,
            "--binary",
            binary,
            "--wrapper",
            self.wrapper,
        ]

        for key, val in self.args.items():
            logger.info("Argument from config: {} = {}", key, val)
            if f"--{key}" not in argv:
                if isinstance(val, bool):
                    if val:
                        argv += [f"--{key}"]
                else:
                    argv += [f"--{key}", val]
            else:
                logger.warning("setting {} can not be overriden", key)

        # Run PREP
        logger.info("argv={}", " ".join(argv))
        prep(argv=argv)
        self.archive_logs(["OPTIONS.nam", "LISTING_PREP0.txt"])


class OfflineForecast(SurfexBinaryTask):
    """Running Forecast task.

    Args:
    -----------------------------------------------------------------------
        SurfexBinaryTask(Task): Inheritance of surfex binary task class

    """

    def __init__(self, config):
        """Construct the forecast task.

        Args:
        -----------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, __class__.__name__)

        try:
            self.mode = self.config["task.args.mode"]
        except KeyError:
            raise RuntimeError from KeyError

        if self.mode == "reforecast":
            self.forcing_type = "an_forcing"
            self.basetime = self.basetime - self.fcint
            args = "offline.args"
        elif self.mode == "cycle":
            self.forcing_type = "default"
            args = "offline.args"
        elif self.mode == "forecast":
            self.forcing_type = "forecast"
            args = "offline.forecast.args"
        else:
            raise NotImplementedError
        try:
            self.args = self.config[f"{args}"]
        except KeyError:
            self.args = {}
        try:
            self.wrapper = f"{self.config['submission.task.wrapper']}"
        except KeyError:
            self.wrapper = ""

    def execute(self):
        """Execute."""
        # Create namelist
        nml_file = "OPTIONS_input.nam"
        settings = SettingsFromNamelistAndConfig("offline", self.config)
        settings.nam_gen.write(nml_file)

        csurffile = settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")
        ctimeseries_filetype = settings.get_setting("NAM_IO_OFFLINE#CTIMESERIES_FILETYPE")

        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"
        archive_path = f"{self.archive_path}"
        archive = self.platform.substitute(archive_path, basetime=self.basetime)
        if self.mode == "forecast":
            archive = f"{archive}/forecast/"

        binary = self.get_binary("OFFLINE")

        output = f"{archive}/{csurffile}{self.suffix}"
        try:
            archive_data = self.args["archive-data"]
        except KeyError:
            archive_data = None

        if self.mode in ("reforecast", "cycle"):
            forecast_range = as_timedelta(self.config["general.times.cycle_length"])
        else:
            forecast_range = as_timedelta(self.config["general.times.forecast_range"])
        xtstep_output = settings.nml["nam_io_offline"]["xtstep_output"]
        if "output-frequency" in self.args:
            xtstep_output = int(self.args["output-frequency"])
        dt = as_timedelta(f"PT{int(xtstep_output)}S")

        if archive_data is None:
            data_dict = {}
        else:
            with open(archive_data, mode="r", encoding="utf8") as fhandler:
                data_dict = json.load(fhandler)

        validtime = self.basetime + dt
        while validtime <= (self.basetime + forecast_range):
            ymd = validtime.strftime("%Y%m%d")
            chour = validtime.strftime("%H")
            cmin = validtime.strftime("%M")
            fname = f"{csurffile}.{ymd}_{chour}h{cmin}.nc"
            if ctimeseries_filetype == "NC":
                fname = f"{csurffile}.{ymd}_{chour}h{cmin}.nc"
                data_dict.update({fname: f"{archive}/{fname}"})
            else:
                logger.warning("Only NC archiving implemented")
            validtime += dt
        archive_data = "archive_data.json"
        with open(archive_data, mode="w", encoding="utf8") as fhandler:
            json.dump(data_dict, fhandler)

        # Forcing dir
        forcing_dir = self.config["system.forcing_dir"]
        forcing_dir = f"{forcing_dir}/{self.forcing_type}"
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.basetime)

        # Offline arguments output
        argv = [
            "--system-file-paths",
            self.get_exp_file_paths_file(),
            "--pgd",
            pgd_file_path,
            "--prep",
            self.get_forecast_start_file(self.basetime, self.mode),
            "--basetime",
            self.basetime.strftime("%Y%m%d%H"),
            "--namelist-path",
            nml_file,
            "--input-binary-data",
            self.input_definition,
            "--forcing-dir",
            forcing_dir,
            "--output",
            output,
            "--wrapper",
            self.wrapper,
            "--binary",
            binary,
        ]
        if archive_data is not None:
            argv += ["--archive", archive_data]
        for key, val in self.args.items():
            if f"--{key}" not in argv:
                if isinstance(val, bool):
                    if val:
                        argv += [f"--{key}"]
                else:
                    argv += [f"--{key}", str(val)]
            else:
                logger.warning("setting {} can not be overriden", key)
        if "--output-frequency" not in argv:
            argv += ["--output-frequency", str(self.fcint.total_seconds())]

        # Run Offline
        offline(argv=argv)


class PerturbedRun(SurfexBinaryTask):
    """Running a perturbed forecast task.

    Args:
    ------------------------------------------------------------------------
        SurfexBinaryTask(Task): Inheritance of surfex binary task class

    """

    def __init__(self, config):
        """Construct a perturbed run task.

        Args:
        --------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, __class__.__name__)
        self.mode = "perturbed"
        try:
            self.pert = self.config["task.args.pert"]
        except KeyError:
            raise RuntimeError from KeyError
        self.negpert = False
        try:
            pert_sign = self.config["task.args.pert_sign"]
        except KeyError:
            pert_sign = "pos"
        if pert_sign == "neg":
            self.negpert = True
        try:
            self.args = self.config[f"{self.mode}.args"]
        except KeyError:
            self.args = {}
        try:
            self.wrapper = f"{self.config['submission.task.wrapper']}"
        except KeyError:
            self.wrapper = ""

    def execute(self):
        """Execute."""
        # Create namelist
        nml_file = "OPTIONS_input.nam"
        settings = SettingsFromNamelistAndConfig("offline", self.config)
        settings.nam_gen.write(nml_file)

        csurffile = settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")
        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"
        binary = self.get_binary("OFFLINE")

        archive = f"{self.platform.get_system_value('archive_dir')}"
        prepfile = self.get_forecast_start_file(self.fg_basetime, "perturbed")
        output = f"{archive}/{csurffile}_PERT{self.pert!s}{self.suffix}"

        # Forcing dir is for previous cycle
        # TODO If perturbed runs moved to pp it should be a diffenent dtg
        forcing_dir = self.config["system.forcing_dir"]
        forcing_dir = f"{forcing_dir}/default"
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.fg_basetime)

        # Offline arguments output
        argv = [
            "--system-file-paths",
            self.get_exp_file_paths_file(),
            "--pgd",
            pgd_file_path,
            "--prep",
            prepfile,
            "--basetime",
            self.basetime.strftime("%Y%m%d%H"),
            "--namelist-path",
            nml_file,
            "--input-binary-data",
            self.input_definition,
            "--forcing-dir",
            forcing_dir,
            "--binary",
            binary,
            "--pert",
            self.pert,
            "--wrapper",
            self.wrapper,
            "--output",
            output,
        ]
        if self.negpert:
            argv += ["--negpert"]
        for key, val in self.args.items():
            if f"--{key}" not in argv:
                if isinstance(val, bool):
                    if val:
                        argv += [f"--{key}"]
                else:
                    argv += [f"--{key}", val]
            else:
                logger.warning("setting {} can not be overriden", key)

        # Run Offline
        perturbed_offline(argv=argv)


class Soda(SurfexBinaryTask):
    """Running SODA (Surfex Offline Data Assimilation) task.

    Args:
    ---------------------------------------------------------------------
        SurfexBinaryTask(Task): Inheritance of surfex binary task class

    """

    def __init__(self, config):
        """Construct a Soda task.

        Args:
        -------------------------------------------------------
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, __class__.__name__)
        self.mode = "soda"
        try:
            self.args = self.config[f"{self.mode}.args"]
        except KeyError:
            self.args = {}
        try:
            self.wrapper = f"{self.config['submission.task.wrapper']}"
        except KeyError:
            self.wrapper = ""

    def execute(self):
        """Execute."""
        # Create namelist
        nml_file = "OPTIONS_input.nam"
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        nnco = settings.get_nnco(self.config, self.basetime)
        settings.nml["nam_obs"]["nnco"] = nnco
        settings.nam_gen.write(nml_file)

        cassim_isba = settings.get_setting("NAM_ASSIM#CASSIM_ISBA")
        binary = self.get_binary("SODA")
        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"

        archive = self.platform.get_system_value("archive_dir")
        prep_file_path = self.get_first_guess(self.basetime)
        output = archive + "/ANALYSIS" + self.suffix
        if cassim_isba == "EKF":
            # TODO If pertubed runs moved to pp it should be a diffenent dtg
            archive_dir = self.config["system.archive_dir"]
            pert_run_dir = self.platform.substitute(archive_dir, basetime=self.basetime)
            self.exp_file_paths.add_system_file_path("perturbed_run_dir", pert_run_dir)
            first_guess_dir = self.platform.substitute(
                archive_dir, basetime=self.fg_basetime
            )
            self.exp_file_paths.add_system_file_path("first_guess_dir", first_guess_dir)

        # Soda arguments output
        argv = [
            "--system-file-paths",
            self.get_exp_file_paths_file(),
            "--pgd",
            pgd_file_path,
            "--prep",
            prep_file_path,
            "--basetime",
            self.basetime.strftime("%Y%m%d%H"),
            "--namelist-path",
            nml_file,
            "--input-binary-data",
            self.input_definition,
            "--output",
            output,
            "--wrapper",
            self.wrapper,
            "--binary",
            binary,
        ]
        for key, val in self.args.items():
            if f"--{key}" not in argv:
                if isinstance(val, bool):
                    if val:
                        argv += [f"--{key}"]
                else:
                    argv += [f"--{key}", val]
            else:
                logger.warning("setting {} can not be overriden", key)

        # Run Soda
        soda(argv=argv)
