"""Tasks running surfex binaries."""
import os
from datetime import timedelta
import logging


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
            logging.info("No mode provided")
            return
        self.mode = mode
        self.need_pgd = True
        self.need_prep = True
        self.pgd = False
        self.prep = False
        self.perturbed = False
        self.soda = False
        self.namelist = None

        kwargs = self.config.get_setting("TASK#ARGS")
        logging.debug("kwargs: %s", kwargs)
        print_namelist = kwargs.get("print_namelist")
        if print_namelist is None:
            print_namelist = True

        self.print_namelist = print_namelist
        check_existence = kwargs.get("check_existence")
        if check_existence is None:
            check_existence = True
        self.check_existence = check_existence
        logging.debug("check_existence %s", check_existence)

        force = kwargs.get("force")
        if force is None:
            force = False
        self.force = force

        pert = kwargs.get("pert")
        if pert is not None:
            pert = int(pert)
        self.pert = pert
        logging.debug("Pert %s", self.pert)

        self.ivar = kwargs.get("ivar")

        xyz = ".exe"
        libdir = self.config.system.get_var("SFX_EXP_LIB", self.config.host)
        xyz_file = libdir + "/xyz"
        if os.path.exists(xyz_file):
            with open(xyz_file, mode="r", encoding="utf-8") as zyz_fh:
                xyz = zyz_fh.read().rstrip()
        else:
            logging.info("%s not found. Assume XYZ=%s", xyz_file, xyz)
        self.xyz = xyz

    def execute(self):
        """Execute task.
        """
        logging.debug("Using empty class execute")


    def execute_binary(self, binary, output, pgd_file_path=None, prep_file_path=None,
                       archive_data=None, forc_zs=False,
                       masterodb=True, perturbed_file_pattern=None, fcint=3, prep_file=None,
                       prep_filetype=None,
                       prep_pgdfile=None, prep_pgdfiletype=None):
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
            input_data = surfex.PgdInputData(self.config, self.exp_file_paths,
                                             check_existence=self.check_existence)
        elif self.mode == "prep":
            self.prep = True
            self.need_prep = False
            input_data = surfex.PrepInputData(self.config, self.exp_file_paths,
                                              check_existence=self.check_existence,
                                              prep_file=prep_file, prep_pgdfile=prep_pgdfile)
        elif self.mode == "offline":
            input_data = surfex.OfflineInputData(self.config, self.exp_file_paths,
                                                 check_existence=self.check_existence)
        elif self.mode == "soda":
            self.soda = True
            # print(kwargs)
            input_data = surfex.SodaInputData(self.config, self.exp_file_paths,
                                              check_existence=self.check_existence,
                                              dtg=self.dtg, masterodb=masterodb,
                                              perturbed_file_pattern=perturbed_file_pattern)
        elif self.mode == "perturbed":
            self.perturbed = True
            input_data = surfex.OfflineInputData(self.config, self.exp_file_paths,
                                                 check_existence=self.check_existence)
        else:
            raise NotImplementedError(self.mode + " is not implemented!")

        logging.debug("pgd %s", pgd_file_path)
        logging.debug("self.perturbed %s, self.pert %s", self.perturbed, self.pert)

        self.namelist = surfex.BaseNamelist(self.mode, self.config, self.input_path,
                                            forc_zs=forc_zs,
                                            prep_file=prep_file, prep_filetype=prep_filetype,
                                            prep_pgdfile=prep_pgdfile,
                                            prep_pgdfiletype=prep_pgdfiletype,
                                            dtg=self.dtg, fcint=fcint)

        logging.debug("rte %s", str(rte))
        batch = surfex.BatchJob(rte, wrapper=self.wrapper)

        # settings = surfex.ascii2nml(json_settings)
        settings = self.namelist.get_namelist()
        self.geo.update_namelist(settings)

        # Create input
        # my_ecoclimap = surfex.JsonInputDataFromFile(ecoclimap_file)
        filetype = settings["nam_io_offline"]["csurf_filetype"]
        pgdfile = settings["nam_io_offline"]["cpgdfile"]
        prepfile = settings["nam_io_offline"]["cprepfile"]
        surffile = settings["nam_io_offline"]["csurffile"]
        lfagmap = False
        if "LFAGMAP" in settings["NAM_IO_OFFLINE"]:
            lfagmap = settings["NAM_IO_OFFLINE"]["LFAGMAP"]

        if self.need_pgd:
            pgdfile = surfex.file.PGDFile(filetype, pgdfile, input_file=pgd_file_path,
                                          lfagmap=lfagmap)

        if self.need_prep:
            prepfile = surfex.PREPFile(filetype, prepfile, input_file=prep_file_path,
                                       lfagmap=lfagmap)

        if self.need_prep and self.need_pgd:
            surffile = surfex.SURFFile(filetype, surffile, archive_file=output,
                                       lfagmap=lfagmap)
        else:
            surffile = None

        if self.perturbed:
            if self.pert > 0:
                surfex.PerturbedOffline(binary, batch, prepfile, self.ivar, settings, input_data,
                                        pgdfile=pgdfile, surfout=surffile,
                                        archive_data=archive_data,
                                        print_namelist=self.print_namelist)
            else:
                surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data,
                                    pgdfile=pgdfile, surfout=surffile,
                                    archive_data=archive_data,
                                    print_namelist=self.print_namelist)
        elif self.pgd:
            pgdfile = surfex.file.PGDFile(filetype, pgdfile, input_file=pgd_file_path,
                                          archive_file=output, lfagmap=lfagmap)
            # print(input_data.data)
            surfex.SURFEXBinary(binary, batch, pgdfile, settings, input_data,
                                archive_data=archive_data, print_namelist=self.print_namelist)
        elif self.prep:
            prepfile = surfex.PREPFile(filetype, prepfile, archive_file=output,
                                       lfagmap=lfagmap)
            surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data, pgdfile=pgdfile,
                                archive_data=archive_data, print_namelist=self.print_namelist)
        else:
            surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data, pgdfile=pgdfile,
                                surfout=surffile, archive_data=archive_data,
                                print_namelist=self.print_namelist)


class Pgd(SurfexBinaryTask):
    """Running PGD task.

    Args:
        SurfexBinaryTask(AbstractTask): Inheritance of surfex binary task class
    """

    def __init__(self, config, **kwargs):
        """Construct a Pgd task object.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_
        """
        SurfexBinaryTask.__init__(self, config, "pgd",
                                  **kwargs)

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        output = self.exp_file_paths.get_system_file("pgd_dir", pgdfile,
                                                     default_dir="default_climdir")
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
            config (_type_): _description_

        """
        SurfexBinaryTask.__init__(self, config, "prep")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile,
                                                            default_dir="default_climdir")
        prep_file = self.config.get_setting("INITIAL_CONDITIONS#PREP_INPUT_FILE",
                                            validtime=self.dtg, basedtg=self.fg_dtg)
        prep_filetype = self.config.get_setting("INITIAL_CONDITIONS#PREP_INPUT_FILETYPE")
        prep_pgdfile = self.config.get_setting("INITIAL_CONDITIONS#PREP_PGDFILE")
        prep_pgdfiletype = self.config.get_setting("INITIAL_CONDITIONS#PREP_PGDFILETYPE")
        prepfile = self.config.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        output = self.exp_file_paths.get_system_file("prep_dir", prepfile, basedtg=self.dtg,
                                                     default_dir="default_archive_dir")
        binary = self.bindir + "/PREP" + self.xyz

        if not os.path.exists(output) or self.force:

            SurfexBinaryTask.execute_binary(self, binary, output, pgd_file_path=pgd_file_path,
                                            prep_file=prep_file,
                                            prep_filetype=prep_filetype, prep_pgdfile=prep_pgdfile,
                                            prep_pgdfiletype=prep_pgdfiletype)
        else:
            logging.info("Output already exists: %s", output)

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
            config (_type_): _description_
        """
        SurfexBinaryTask.__init__(self, config, "offline")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile,
                                                            default_dir="default_climdir")
        binary = self.bindir + "/OFFLINE" + self.xyz
        forc_zs = self.config.get_setting("FORECAST#FORC_ZS")

        output = self.archive + "/" + self.config.get_setting("SURFEX#IO#CSURFFILE") + self.suffix

        archive_data = None
        if self.config.get_setting("SURFEX#IO#CTIMESERIES_FILETYPE") == "NC":
            last_ll = self.dtg + timedelta(seconds=self.fcint)

            logging.debug("LAST_LL: %s", last_ll)
            fname = "SURFOUT." + last_ll.strftime("%Y%m%d") + "_" + last_ll.strftime("%H") + "h" + \
                    last_ll.strftime("%M") + ".nc"
            logging.debug("Filename: %s", fname)
            archive_data = surfex.JsonOutputData({fname: self.archive + "/" + fname})
            logging.debug("archive_data=%s", archive_data)

        # Forcing dir
        self.exp_file_paths.add_system_file_path("forcing_dir", self.exp_file_paths.get_system_path(
            "forcing_dir", mbr=self.mbr, basedtg=self.dtg, default_dir="default_forcing_dir"))

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary, output,
                                            pgd_file_path=pgd_file_path,
                                            prep_file_path=self.fc_start_sfx,
                                            forc_zs=forc_zs, archive_data=archive_data)
        else:
            logging.info("Output already exists: %s", output)

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
            config (dict): _description_

        """
        SurfexBinaryTask.__init__(self, config, "perturbed")

    def execute(self):
        """Execute."""
        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile,
                                                            default_dir="default_climdir")
        bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")
        binary = bindir + "/OFFLINE" + self.xyz
        forc_zs = self.config.get_setting("FORECAST#FORC_ZS")

        # PREP file is previous analysis unless first assimilation cycle
        if self.fg_dtg == self.dtgbeg:
            prepfile = self.config.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        else:
            prepfile = "ANALYSIS" + self.suffix

        prep_file_path = self.exp_file_paths.get_system_file("prep_dir", prepfile, mbr=self.mbr,
                                                             basedtg=self.fg_dtg,
                                                             default_dir="default_archive_dir")

        output = self.archive + "/" + self.config.get_setting("SURFEX#IO#CSURFFILE") + "_PERT" + \
                                str(self.pert) + self.suffix

        # Forcing dir is for previous cycle
        # TODO If pertubed runs moved to pp it should be a diffenent dtg
        forcing_path = self.exp_file_paths.get_system_path("forcing_dir", mbr=self.mbr,
                                                           basedtg=self.fg_dtg,
                                                           default_dir="default_forcing_dir")
        self.exp_file_paths.add_system_file_path("forcing_dir", forcing_path)

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary, output,
                                            pgd_file_path=pgd_file_path,
                                            forc_zs=forc_zs,
                                            prep_file_path=prep_file_path)
        else:
            logging.info("Output already exists: ", output)

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
            config (_type_): _description_
        """
        SurfexBinaryTask.__init__(self, config, "soda")

    def execute(self):
        """Execute."""
        bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")
        binary = bindir + "/SODA" + self.xyz

        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile,
                                                            default_dir="default_climdir")

        prep_file_path = self.fg_guess_sfx
        output = self.archive + "/ANALYSIS" + self.suffix
        perturbed_file_pattern = None
        if self.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "EKF"):
            # TODO If pertubed runs moved to pp it should be a diffenent dtg
            pert_run_dir = self.exp_file_paths.get_system_path("archive_dir",
                                                               default_dir="default_archive_dir")
            self.exp_file_paths.add_system_file_path("perturbed_run_dir", pert_run_dir,
                                                     mbr=self.mbr, basedtg=self.dtg)
            first_guess_dir = self.exp_file_paths.get_system_path("default_first_guess_dir",
                                                                  mbr=self.mbr,
                                                                  validtime=self.dtg,
                                                                  basedtg=self.fg_dtg)
            self.exp_file_paths.add_system_file_path("first_guess_dir", first_guess_dir)

            csurffile = self.config.get_setting("SURFEX#IO#CSURFFILE")
            perturbed_file_pattern = csurffile + "_PERT@PERT@" + self.suffix

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary, output, pgd_file_path=pgd_file_path,
                                            prep_file_path=prep_file_path,
                                            perturbed_file_pattern=perturbed_file_pattern)
        else:
            logging.info("Output already exists: ", output)

        # SODA should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)

    # Make sure we don't clean yet
    def postfix(self):
        """Do no postfix."""
