"""Tasks running surfex binaries."""
import json
import os
import f90nml

from pysurfex.cli import run_surfex_binary

from deode.datetime_utils import as_datetime, as_timedelta, cycle_offset, get_decade
from deode.namelist import NamelistGenerator
from deode.os_utils import deodemakedirs
from deode.tasks.marsprep import Marsprep

from surfexp.tasks.tasks import PySurfexBaseTask
from surfexp.experiment import SettingsFromNamelistAndConfig


class SurfexBinaryTask(PySurfexBaseTask):
    """Task."""

    def __init__(self, config, name):
        """Construct object.

        Args:
            config (deode.ParsedConfig): Configuration

        """
        PySurfexBaseTask.__init__(self, config, name)
        self.one_decade = self.config["pgd.one_decade"]

    def get_pgdfile(self, basetime):
        if self.one_decade:
            decade = f"_{get_decade(as_datetime(basetime))}"
        else:
            decade = ""
        pgdfile = f"{self.climdir}/PGD{decade}{self.suffix}"
        return pgdfile

    def execute(self):
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
        self.task_basetime = config["task.args.basetime"]

        self.mode = "pgd"
        # TODO get from args
        self.force = True

    def execute(self):
        """Execute."""

        # Create namelist the deode way
        nml_file = "OPTIONS_input.nam"
        #self.nlgen.load(self.mode)
        #settings = self.nlgen.assemble_namelist(self.mode)
        #self.nlgen.write_namelist(settings, nml_file)
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        settings.nam_gen.write(nml_file)

        # TODO use task_basetime
        output = f"{self.get_pgdfile(self.basetime)}"
        binary = self.get_binary("PGD")

        exp_file_paths_file = "exp_file_paths.json"
        #TODO save this file in pysurfex
        json.dump(self.exp_file_paths.system_file_paths, open(exp_file_paths_file, mode="w", encoding="utf8"))

        try:
            wrapper = self.config["submission.task.wrapper"]
        except KeyError:
            wrapper = ""
        # PGD arguments
        kwargs = {
            "domain": self.domain_file,
            "system_file_paths": exp_file_paths_file,
            "basetime": self.task_basetime,
            "force": self.force,
            "namelist_path": nml_file,
            "input_binary_data": self.input_definition,
            "tolerate_missing": True,
            "binary": binary,
            "rte": None,
            "wrapper": wrapper,
            "output": output,
            "masterodb": False,
            "archive": None,
            "print_namelist": True,
            "one_decade": True
        }

        # Run PGD
        run_surfex_binary(self.mode, **kwargs)
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
        # TODO get from args
        self.force = True

    def execute(self):
        """Execute."""
        cnmexp = self.config["general.cnmexp"]
        output = f"{self.archive}/ICMSH{cnmexp}INIT.sfx"

        binary = self.get_binary("PREP")
        deodemakedirs(self.archive)

        exp_file_paths_file = "exp_file_paths.json"
        #TODO save this file in pysurfex
        json.dump(self.exp_file_paths.system_file_paths, open(exp_file_paths_file, mode="w", encoding="utf8"))


        # Create namelist the deode way
        #self.nlgen.load(self.mode)
        #settings = self.nlgen.assemble_namelist(self.mode)
        nml_file = "OPTIONS_input.nam"
        #self.nlgen.write_namelist(settings, nml_file)
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        settings.nam_gen.write(nml_file)

        exp_file_paths_file = "exp_file_paths.json"
        #TODO save this file in pysurfex
        json.dump(self.exp_file_paths.system_file_paths, open(exp_file_paths_file, mode="w", encoding="utf8"))

        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"

        try:
            prep_file = self.config["initial_conditions.prep_input_file"]
        except AttributeError:
            prep_file = None
        if prep_file is not None:
            if prep_file == "":
                prep_file = None
            else:
                prep_file = self.platform.substitute(
                    prep_file, validtime=self.dtg, basetime=self.fg_dtg
                )
        try:
            prep_pgdfile = self.config["initial_conditions.prep_pgdfile"]
        except AttributeError:
            prep_pgdfile = None
        if prep_pgdfile == "":
            prep_pgdfile = None
        cprepfile = self.soda_settings.get_setting("NAM_IO_OFFLINE#CPREPFILE")
        cprepfile = f"{cprepfile}{self.suffix}"

        archive = self.platform.get_system_value("archive_dir")
        output = f"{self.platform.substitute(archive, basetime=self.dtg)}/{cprepfile}"

        # PREP arguments output
        try:
            wrapper = self.config["submission.task.wrapper"]
        except KeyError:
            wrapper = ""
        kwargs = {
            "system_file_paths": exp_file_paths_file,
            "pgd": pgd_file_path,
            "prep_file": prep_file,
            "prep_pgdfile": prep_pgdfile,
            "basetime": self.basetime,
            "force": self.force,
            "namelist_path": nml_file,
            "input_binary_data": self.input_definition,
            "tolerate_missing": False,
            "output": output,
            "binary": binary,
            "rte": None,
            "wrapper": wrapper,
            "output": output,
            "masterodb": False,
            "archive": None,
            "print_namelist": True,
        }

        # Run PREP
        run_surfex_binary(self.mode, **kwargs)
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
        self.mode = "offline"
        # TODO get from args
        self.force = True


    def execute(self):
        """Execute."""

        # Create namelist the deode way
        #self.nlgen.load(self.mode)
        #settings = self.nlgen.assemble_namelist(self.mode)
        nml_file = "OPTIONS_input.nam"
        #self.nlgen.write_namelist(settings, nml_file)
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        settings.nam_gen.write(nml_file)

        pgd_file_path = f"{self.climdir}/PGD_@DECADE@.nc"

        exp_file_paths_file = "exp_file_paths.json"
        #TODO save this file in pysurfex
        json.dump(self.exp_file_paths.system_file_paths, open(exp_file_paths_file, mode="w", encoding="utf8"))


        cpgdfile = settings.get_setting("NAM_IO_OFFLINE#CPGDFILE")
        csurffile = settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")
        ctimeseries_filetype = settings.get_setting("NAM_IO_OFFLINE#CTIMESERIES_FILETYPE")

        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"
        archive = f"{self.platform.get_system_value('archive_dir')}"

        binary = self.get_binary("OFFLINE")

        output = f"{archive}/{csurffile}{self.suffix}"
        archive_data = None
        if ctimeseries_filetype == "NC":
            last_ll = self.basetime + self.fcint

            fname = (
                "SURFOUT."
                + last_ll.strftime("%Y%m%d")
                + "_"
                + last_ll.strftime("%H")
                + "h"
                + last_ll.strftime("%M")
                + ".nc"
            )
            archive_data = "archive_data.json"
            json.dump({fname: archive + "/" + fname}, open(archive_data, mode="w", encoding="utf8"))

        # Forcing dir
        forcing_dir = self.platform.get_system_value("forcing_dir")
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.dtg)

        # Offline arguments output
        try:
            wrapper = self.config["submission.task.wrapper"]
        except KeyError:
            wrapper = ""
        kwargs = {
            "system_file_paths": exp_file_paths_file,
            "pgd": pgd_file_path,
            "prep": self.fc_start_sfx,
            "basetime": self.basetime,
            "force": self.force,
            "namelist_path": nml_file,
            "input_binary_data": self.input_definition,
            "tolerate_missing": False,
            "forcing_dir": forcing_dir,
            "rte": None,
            "archive": archive_data,
            "print_namelist": True,
            "masterodb": False,
            "output": output,
            "wrapper": wrapper,
            "binary": binary
        }

        # Run Offline
        run_surfex_binary(self.mode, **kwargs)


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
        # TODO get from args
        self.force = True


    def execute(self):
        """Execute."""

        # Create namelist the deode way
        #self.nlgen.load(self.mode)
        #settings = self.nlgen.assemble_namelist("offline")
        nml_file = "OPTIONS_input.nam"
        #self.nlgen.write_namelist(settings, nml_file)
        settings = SettingsFromNamelistAndConfig("offline", self.config)
        settings.nam_gen.write(nml_file)

        exp_file_paths_file = "exp_file_paths.json"
        #TODO save this file in pysurfex
        json.dump(self.exp_file_paths.system_file_paths, open(exp_file_paths_file, mode="w", encoding="utf8"))

        cpgdfile = settings.get_setting("NAM_IO_OFFLINE#CPGDFILE")
        cprepfile = settings.get_setting("NAM_IO_OFFLINE#CPREPFILE")
        csurffile = settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")

        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"
        binary = self.get_binary("OFFLINE")

        # PREP file is previous analysis unless first assimilation cycle
        if self.fg_dtg == as_datetime(self.config["general.times.start"]):
            try:
                prepfile = f"{cprepfile}{self.suffix}"
            except KeyError:
                raise RuntimeError from KeyError
        else:
            prepfile = "ANALYSIS" + self.suffix

        archive_pattern = self.config["system.archive_dir"]
        prep_file_path = self.platform.substitute(archive_pattern, basetime=self.fg_dtg)
        prep_file_path = f"{prep_file_path}/{prepfile}"
        output = f"{self.archive}/{csurffile}_PERT{self.pert!s}{self.suffix}"

        # Forcing dir is for previous cycle
        # TODO If perturbed runs moved to pp it should be a diffenent dtg
        forcing_dir = self.config["system.forcing_dir"]
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.fg_dtg)

        try:
            wrapper = self.config["submission.task.wrapper"]
        except KeyError:
            wrapper = ""
        # Offline arguments output
        kwargs = {
            "system_file_paths": exp_file_paths_file,
            "pgd": pgd_file_path,
            "prep": self.fc_start_sfx,
            "basetime": self.basetime,
            "force": self.force,
            "namelist_path": nml_file,
            "input_binary_data": self.input_definition,
            "tolerate_missing": False,
            "forcing_dir": forcing_dir,
            "archive": None,
            "output": output,
            "binary": binary,
            "rte": None,
            "pert": self.pert,
            "wrapper": wrapper,
            "output": output,
            "masterodb": False,
            "archive": None,
            "print_namelist": True,
            "negpert": self.negpert
        }

        # Run Offline
        run_surfex_binary(self.mode, **kwargs)


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
        # TODO get from args
        self.force = True

    def execute(self):
        """Execute."""


        # Create namelist the deode way
        #self.nlgen.load(self.mode)
        #settings = self.nlgen.assemble_namelist(self.mode)
        nml_file = "OPTIONS_input.nam"
        #self.nlgen.write_namelist(settings, nml_file)
        settings = SettingsFromNamelistAndConfig(self.mode, self.config)
        settings.nam_gen.write(nml_file)

        pgd_file_path = f"{self.climdir}/{self.suffix}"

        #csurf_filetype = settings.get_setting("NAM_IO_OFFLINE#CSURF_FILETYPE", default="NC")
        cpgdfile = settings.get_setting("NAM_IO_OFFLINE#CPGDFILE")
        #cprepfile = settings.get_setting("NAM_IO_OFFLINE#CPREPFILE")
        #csurffile = settings.get_setting("NAM_IO_OFFLINE#CSURFFILE")
        cassim_isba = settings.get_setting("NAM_ASSIM#CASSIM_ISBA")


        binary = self.get_binary("SODA" + self)
        pgd_file_path = f"{self.get_pgdfile(self.basetime)}"

        archive = self.platform.get_system_value("archive_dir")
        prep_file_path = self.fg_guess_sfx
        output = archive + "/ANALYSIS" + self.suffix
        if cassim_isba == "EKF":
            # TODO If pertubed runs moved to pp it should be a diffenent dtg
            archive_dir = self.config["system.archive_dir"]
            pert_run_dir = self.platform.substitute(archive_dir, basetime=self.basetime)
            self.exp_file_paths.add_system_file_path("perturbed_run_dir", pert_run_dir)
            first_guess_dir = self.platform.substitute(archive_dir, basetime=self.fg_dtg)
            self.exp_file_paths.add_system_file_path("first_guess_dir", first_guess_dir)

        archive_data = None
        # Offline arguments output
        try:
            wrapper = self.config["submission.task.wrapper"]
        except KeyError:
            wrapper = ""
        kwargs = {
            "system_file_paths": self.exp_file_paths,
            "pgd": pgd_file_path,
            "prep": prep_file_path,
            "basetime": self.basetime,
            "force": self.force,
            "namelist_path": nml_file,
            "input_binary_data": self.input_definition,
            "tolerate_missing": False,
            "archive": archive_data,
            "output": output,
            "wrapper": wrapper,
            "binary": binary
        }

        # Run Soda
        run_surfex_binary(self.mode, **kwargs)

        # SODA should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)
