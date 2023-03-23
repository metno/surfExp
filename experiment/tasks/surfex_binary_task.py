"""Tasks running surfex binaries."""
import os

import surfex

from ..tasks.tasks import AbstractTask


class SurfexBinaryTask(AbstractTask):
    """Main surfex binary task executing all tasks.

    Args:
        AbstractTask (object): Inheritance of base task class
    """

    def __init__(self, config, mode=None):
        """Construct a surfex binary task.

        Args:
            config (_type_): _description_
            mode (_type_): _description_

        """
        AbstractTask.__init__(self, config)

        if mode is None:
            self.logger.info("No mode provided")
            return
        self.mode = mode
        self.need_pgd = True
        self.need_prep = True
        self.pgd = False
        self.prep = False
        self.perturbed = False
        self.soda = False
        self.namelist = None
        # SURFEX config added to general config
        cfg = self.config.get_value("SURFEX").dict()
        sfx_config = {"SURFEX": cfg}
        self.sfx_config = surfex.Configuration(sfx_config)

        # Get paths from file manager (example)
        ecosg_dir = self.fmanager.platform.get_platform_value("ecosg_data_path")
        pgd_dir = self.fmanager.platform.get_platform_value("pgd_data_path")
        climdir = self.fmanager.platform.get_system_value("climdir")

        # TODO get all needed paths
        exp_file_paths = self.config.get_value("system").dict()
        obs_dir = self.platform.get_system_value("obs_dir")
        # To sub EEE/RRR
        obs_dir = self.platform.substitute(obs_dir)
        exp_file_paths.update(
            {
                "obs_dir": obs_dir,
                "tree_height_dir": f"{ecosg_dir}/HT/",
                "flake_dir": f"{pgd_dir}",
                "sand_dir": f"{climdir}",
                "clay_dir": f"{climdir}",
                "soc_top_dir": f"{climdir}",
                "soc_sub_dir": f"{climdir}",
                "ecoclimap_sg_cover_dir": f"{ecosg_dir}/COVER/",
                "albnir_soil_dir": f"{ecosg_dir}/ALB_SAT/",
                "albvis_soil_dir": f"{ecosg_dir}/ALB_SAT/",
                "albnir_veg_dir": f"{ecosg_dir}/ALB_SAT/",
                "albvis_veg_dir": f"{ecosg_dir}/ALB_SAT/",
                "lai_dir": f"{ecosg_dir}/LAI_SAT/",
                "oro_dir": f"{climdir}",
            }
        )
        self.exp_file_paths = surfex.SystemFilePaths(exp_file_paths)

        kwargs = self.config.get_value("task.args").dict()
        self.logger.debug("kwargs: %s", kwargs)
        print_namelist = kwargs.get("print_namelist")
        if print_namelist is None:
            print_namelist = True

        self.print_namelist = print_namelist
        check_existence = kwargs.get("check_existence")
        if check_existence is None:
            check_existence = True
        self.check_existence = check_existence
        self.logger.debug("check_existence %s", check_existence)

        force = kwargs.get("force")
        if force is None:
            force = False
        self.force = force

        pert = kwargs.get("pert")
        if pert is not None:
            pert = int(pert)
        self.pert = pert
        self.logger.debug("Pert %s", self.pert)

        self.ivar = kwargs.get("ivar")

        xyz = ".exe"
        libdir = self.platform.get_system_value("sfx_exp_lib")
        xyz_file = libdir + "/xyz"
        if os.path.exists(xyz_file):
            with open(xyz_file, mode="r", encoding="utf-8") as zyz_fh:
                xyz = zyz_fh.read().rstrip()
        else:
            self.logger.info("%s not found. Assume XYZ=%s", xyz_file, xyz)
        self.xyz = xyz

    def execute(self):
        """Execute task."""
        self.logger.debug("Using empty class execute")

    def execute_binary(
        self,
        binary,
        output,
        pgd_file_path=None,
        prep_file_path=None,
        archive_data=None,
        forc_zs=False,
        masterodb=True,
        perturbed_file_pattern=None,
        fcint=3,
        prep_file=None,
        prep_filetype=None,
        prep_pgdfile=None,
        prep_pgdfiletype=None,
    ):
        """Execute the surfex binary.

        Args:
            binary (str): Full path to binary
            output (str): Full path to output file
            pgd_file_path (str, optional): _description_. Defaults to None.
            prep_file_path (str, optional): _description_. Defaults to None.
            archive_data (surfex.OutputDataFromSurfexBinaries, optional): A mapping of produced
                files and where to archive them. Defaults to None.
            forc_zs (bool, optional): _description_. Defaults to False.
            masterodb (bool, optional): _description_. Defaults to True.
            perturbed_file_pattern (_type_, optional): _description_. Defaults to None.
            fcint (int, optional): _description_. Defaults to 3.
            prep_file (_type_, optional): _description_. Defaults to None.
            prep_filetype (_type_, optional): _description_. Defaults to None.
            prep_pgdfile (_type_, optional): _description_. Defaults to None.
            prep_pgdfiletype (_type_, optional): _description_. Defaults to None.

        Raises:
            NotImplementedError: Unknown implementation.
        """
        rte = os.environ
        if self.mode == "pgd":
            self.pgd = True
            self.need_pgd = False
            self.need_prep = False
            input_data = surfex.PgdInputData(
                self.sfx_config, self.exp_file_paths, check_existence=self.check_existence
            )
        elif self.mode == "prep":
            self.prep = True
            self.need_prep = False
            input_data = surfex.PrepInputData(
                self.sfx_config,
                self.exp_file_paths,
                check_existence=self.check_existence,
                prep_file=prep_file,
                prep_pgdfile=prep_pgdfile,
            )
        elif self.mode == "offline":
            input_data = surfex.OfflineInputData(
                self.sfx_config, self.exp_file_paths, check_existence=self.check_existence
            )
        elif self.mode == "soda":
            self.soda = True
            input_data = surfex.SodaInputData(
                self.sfx_config,
                self.exp_file_paths,
                check_existence=self.check_existence,
                dtg=self.dtg,
                masterodb=masterodb,
                perturbed_file_pattern=perturbed_file_pattern,
            )
        elif self.mode == "perturbed":
            self.perturbed = True
            input_data = surfex.OfflineInputData(
                self.sfx_config, self.exp_file_paths, check_existence=self.check_existence
            )
        else:
            raise NotImplementedError(self.mode + " is not implemented!")

        self.logger.debug("pgd %s", pgd_file_path)
        self.logger.debug("self.perturbed %s, self.pert %s", self.perturbed, self.pert)

        self.namelist = surfex.BaseNamelist(
            self.mode,
            self.sfx_config,
            self.input_path,
            forc_zs=forc_zs,
            prep_file=prep_file,
            prep_filetype=prep_filetype,
            prep_pgdfile=prep_pgdfile,
            prep_pgdfiletype=prep_pgdfiletype,
            dtg=self.dtg,
            fcint=fcint,
        )

        self.logger.debug("rte %s", str(rte))
        batch = surfex.BatchJob(rte, wrapper=self.wrapper)

        settings = self.namelist.get_namelist()
        self.geo.update_namelist(settings)

        # Create input
        filetype = settings["nam_io_offline"]["csurf_filetype"]
        pgdfile = settings["nam_io_offline"]["cpgdfile"]
        prepfile = settings["nam_io_offline"]["cprepfile"]
        surffile = settings["nam_io_offline"]["csurffile"]
        lfagmap = False
        if "lfagmap" in settings["nam_io_offline"]:
            lfagmap = settings["nam_io_offline"]["lfagmap"]

        if self.need_pgd:
            pgdfile = surfex.file.PGDFile(
                filetype, pgdfile, input_file=pgd_file_path, lfagmap=lfagmap
            )

        if self.need_prep:
            prepfile = surfex.PREPFile(
                filetype, prepfile, input_file=prep_file_path, lfagmap=lfagmap
            )

        if self.need_prep and self.need_pgd:
            surffile = surfex.SURFFile(
                filetype, surffile, archive_file=output, lfagmap=lfagmap
            )
        else:
            surffile = None

        if self.perturbed:
            if self.pert > 0:
                surfex.PerturbedOffline(
                    binary,
                    batch,
                    prepfile,
                    self.ivar,
                    settings,
                    input_data,
                    pgdfile=pgdfile,
                    surfout=surffile,
                    archive_data=archive_data,
                    print_namelist=self.print_namelist,
                )
            else:
                surfex.SURFEXBinary(
                    binary,
                    batch,
                    prepfile,
                    settings,
                    input_data,
                    pgdfile=pgdfile,
                    surfout=surffile,
                    archive_data=archive_data,
                    print_namelist=self.print_namelist,
                )
        elif self.pgd:
            pgdfile = surfex.file.PGDFile(
                filetype,
                pgdfile,
                input_file=pgd_file_path,
                archive_file=output,
                lfagmap=lfagmap,
            )
            surfex.SURFEXBinary(
                binary,
                batch,
                pgdfile,
                settings,
                input_data,
                archive_data=archive_data,
                print_namelist=self.print_namelist,
            )
        elif self.prep:
            prepfile = surfex.PREPFile(
                filetype, prepfile, archive_file=output, lfagmap=lfagmap
            )
            surfex.SURFEXBinary(
                binary,
                batch,
                prepfile,
                settings,
                input_data,
                pgdfile=pgdfile,
                archive_data=archive_data,
                print_namelist=self.print_namelist,
            )
        else:
            surfex.SURFEXBinary(
                binary,
                batch,
                prepfile,
                settings,
                input_data,
                pgdfile=pgdfile,
                surfout=surffile,
                archive_data=archive_data,
                print_namelist=self.print_namelist,
            )


class Pgd(SurfexBinaryTask):
    """Running PGD task.

    Args:
        SurfexBinaryTask(AbstractTask): Inheritance of surfex binary task class
    """

    def __init__(self, config):
        """Construct a Pgd task object.

        Args:
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, "pgd")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_value("SURFEX.IO.CPGDFILE") + self.suffix
        output = f"{self.platform.get_system_value('climdir')}/{pgdfile}"
        binary = self.bindir + "/PGD" + self.xyz

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary=binary, output=output)
        else:
            print("Output already exists: ", output)


class Prep(SurfexBinaryTask):
    """Running PREP task.

    Args:
        SurfexBinaryTask(AbstractTask): Inheritance of surfex binary task class
    """

    def __init__(self, config):
        """Construct Prep task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, "prep")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_value("SURFEX.IO.CPGDFILE") + self.suffix
        pgd_file_path = f"{self.platform.get_system_value('climdir')}/{pgdfile}"
        prep_file = self.config.get_value("initial_conditions.prep_input_file")
        prep_file = self.platform.substitute(
            prep_file, validtime=self.dtg, basetime=self.fg_dtg
        )
        prep_filetype = self.config.get_value("initial_conditions.prep_input_filetype")
        prep_pgdfile = self.config.get_value("initial_conditions.prep_pgdfile")
        prep_pgdfiletype = self.config.get_value("initial_conditions.prep_pgdfiletype")
        prepfile = self.config.get_value("SURFEX.IO.CPREPFILE") + self.suffix
        archive = self.platform.get_system_value("archive_dir")
        output = f"{self.platform.substitute(archive, basetime=self.dtg)}/{prepfile}"
        binary = self.bindir + "/PREP" + self.xyz

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(
                self,
                binary,
                output,
                pgd_file_path=pgd_file_path,
                prep_file=prep_file,
                prep_filetype=prep_filetype,
                prep_pgdfile=prep_pgdfile,
                prep_pgdfiletype=prep_pgdfiletype,
            )
        else:
            self.logger.info("Output already exists: %s", output)

        # PREP should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)


class Forecast(SurfexBinaryTask):
    """Running Forecast task.

    Args:
        SurfexBinaryTask(AbstractTask): Inheritance of surfex binary task class
    """

    def __init__(self, config):
        """Construct the forecast task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, "offline")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_value("SURFEX.IO.CPGDFILE") + self.suffix
        pgd_file_path = f"{self.platform.get_system_value('climdir')}/{pgdfile}"
        binary = self.bindir + "/OFFLINE" + self.xyz
        forc_zs = self.config.get_value("forecast.forc_zs")

        output = (
            self.archive
            + "/"
            + self.config.get_value("SURFEX.IO.CSURFFILE")
            + self.suffix
        )

        archive_data = None
        if self.config.get_value("SURFEX.IO.CTIMESERIES_FILETYPE") == "NC":
            last_ll = self.dtg + self.fcint

            self.logger.debug("LAST_LL: %s", last_ll)
            fname = (
                "SURFOUT."
                + last_ll.strftime("%Y%m%d")
                + "_"
                + last_ll.strftime("%H")
                + "h"
                + last_ll.strftime("%M")
                + ".nc"
            )
            self.logger.debug("Filename: %s", fname)
            archive_data = surfex.JsonOutputData({fname: self.archive + "/" + fname})
            self.logger.debug("archive_data=%s", archive_data)

        # Forcing dir
        forcing_dir = self.platform.get_system_value("forcing_dir")
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.basetime)
        self.exp_file_paths.add_system_file_path("forcing_dir", forcing_dir)

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(
                self,
                binary,
                output,
                pgd_file_path=pgd_file_path,
                prep_file_path=self.fc_start_sfx,
                forc_zs=forc_zs,
                archive_data=archive_data,
            )
        else:
            self.logger.info("Output already exists: %s", output)

    def postfix(self):
        """Do default postfix."""


class PerturbedRun(SurfexBinaryTask):
    """Running a perturbed forecast task.

    Args:
        SurfexBinaryTask(AbstractTask): Inheritance of surfex binary task class

    """

    def __init__(self, config):
        """Construct a perturbed run task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, "perturbed")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_value("SURFEX.IO.CPGDFILE") + self.suffix
        pgd_file_path = f"{self.platform.get_system_value('climdir')}/{pgdfile}"
        bindir = self.platform.get_system_value("bin_dir")
        binary = bindir + "/OFFLINE" + self.xyz
        forc_zs = self.config.get_value("forecast.forc_zs")

        # PREP file is previous analysis unless first assimilation cycle
        if self.fg_dtg == self.dtgbeg:
            prepfile = f"{self.config.get_value('SURFEX.IO.CPREPFILE')}{self.suffix}"
        else:
            prepfile = "ANALYSIS" + self.suffix

        archive_pattern = self.config.get_value("system.archive_dir")
        prep_file_path = self.platform.substitute(archive_pattern, basetime=self.fg_dtg)
        prep_file_path = f"{prep_file_path}/{prepfile}"
        surffile = self.config.get_value("SURFEX.IO.CSURFFILE")
        output = f"{self.archive}/{surffile}_PERT{str(self.pert)}{self.suffix}"

        # Forcing dir is for previous cycle
        # TODO If pertubed runs moved to pp it should be a diffenent dtg
        forcing_dir = self.config.get_value("system.forcing_dir")
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.fg_dtg)
        self.exp_file_paths.add_system_file_path("forcing_dir", forcing_dir)

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(
                self,
                binary,
                output,
                pgd_file_path=pgd_file_path,
                forc_zs=forc_zs,
                prep_file_path=prep_file_path,
            )
        else:
            self.logger.info("Output already exists: %s", output)

    # Make sure we don't clean yet
    def postfix(self):
        """Do no postfix."""


class Soda(SurfexBinaryTask):
    """Running SODA (Surfex Offline Data Assimilation) task.

    Args:
        SurfexBinaryTask(AbstractTask): Inheritance of surfex binary task class
    """

    def __init__(self, config):
        """Construct a Soda task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        SurfexBinaryTask.__init__(self, config, "soda")

    def execute(self):
        """Execute."""
        binary = self.bindir + "/SODA" + self.xyz

        pgdfile = self.config.get_value("SURFEX.IO.CPGDFILE") + self.suffix
        pgd_file_path = self.platform.get_system_value("climdir")
        pgd_file_path = f"{self.platform.substitute(pgd_file_path)}/{pgdfile}"

        prep_file_path = self.fg_guess_sfx
        output = self.archive + "/ANALYSIS" + self.suffix
        perturbed_file_pattern = None
        if self.settings.setting_is("SURFEX.ASSIM.SCHEMES.ISBA", "EKF"):
            # TODO If pertubed runs moved to pp it should be a diffenent dtg
            archive_dir = self.config.get_value("system.archive_dir")
            pert_run_dir = self.platform.substitute(archive_dir, basetime=self.dtg)
            self.exp_file_paths.add_system_file_path("perturbed_run_dir", pert_run_dir)
            first_guess_dir = self.platform.substitute(archive_dir, basetime=self.fg_dtg)
            self.exp_file_paths.add_system_file_path("first_guess_dir", first_guess_dir)

            csurffile = self.config.get_value("SURFEX.IO.CSURFFILE")
            perturbed_file_pattern = csurffile + "_PERT@PERT@" + self.suffix

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(
                self,
                binary,
                output,
                pgd_file_path=pgd_file_path,
                prep_file_path=prep_file_path,
                perturbed_file_pattern=perturbed_file_pattern,
            )
        else:
            self.logger.info("Output already exists: %s", output)

        # SODA should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)

    # Make sure we don't clean yet
    def postfix(self):
        """Do no postfix."""
