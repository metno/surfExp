"""Compilation."""
import os
import shutil

from deode.logs import logger
from pysurfex.run import BatchJob

from surfexp.tasks.tasks import PySurfexBaseTask


class SyncSourceCode(PySurfexBaseTask):
    """Sync source code for offline code.

    Args:
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct SyncSourceCode task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "SyncSourceCode")

    def execute(self):
        """Execute."""
        rte = os.environ
        wrapper = ""

        rsync = self.config["compile.rsync"]
        sfx_lib = f"{self.platform.get_system_value('casedir')}/offline"
        os.makedirs(sfx_lib, exist_ok=True)
        offline_source = self.config["compile.ial_source"]
        ifsaux = f"{offline_source}/../../src/ifsaux"
        ifsaux_copy = f"{offline_source}/src/LIB/ifsaux_copy"
        if os.path.exists(ifsaux):
            cmd = f"{rsync} {ifsaux}/* {ifsaux_copy}"
            logger.info(cmd)
            BatchJob(rte, wrapper=wrapper).run(cmd)
        cmd = f"{rsync} {offline_source}/ {sfx_lib}"
        logger.info(cmd)
        BatchJob(rte, wrapper=wrapper).run(cmd)

        # Add system files if not existing
        scripts = self.platform.get_system_value("surfexp")
        host = self.config["compile.build_config"]

        system_file_scripts = f"{scripts}/config/offline/conf/system.{host}"
        system_file_lib = f"{sfx_lib}/conf/system.{host}"

        logger.debug("Check system_file_lib {}", system_file_lib)
        if not os.path.exists(system_file_lib):
            logger.debug("Check system_file_scripts {}", system_file_scripts)
            if os.path.exists(system_file_scripts):
                logger.info("Copy {} {}", system_file_scripts, system_file_lib)
                shutil.copy(system_file_scripts, system_file_lib)

        rules_file_scripts = f"{scripts}/config/offline/src/Rules.{host}.mk"
        rules_file_lib = f"{sfx_lib}/src/Rules.{host}.mk"

        logger.debug("Check rules_file_lib {}", rules_file_lib)
        if not os.path.exists(rules_file_lib):
            logger.debug("Check rules_file_scripts {}", rules_file_scripts)
            if os.path.exists(rules_file_scripts):
                logger.info("Copy {} {}", rules_file_scripts, rules_file_lib)
                shutil.copy(rules_file_scripts, rules_file_lib)


class ConfigureOfflineBinaries(PySurfexBaseTask):
    """Configure offline binaries.

    Args:
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct ConfigureOfflineBinaries task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "ConfigureOfflineBinaries")

    def execute(self):
        """Execute."""
        rte = os.environ
        casedir = f"{self.platform.get_system_value('casedir')}"
        flavour = self.config["compile.build_config"]
        exp_home = self.config["platform.deode_home"]
        cmd = f"{casedir}/offline/scr/Configure_offline.sh {casedir}/offline {flavour}"
        logger.debug(cmd)
        BatchJob(rte, wrapper=self.wrapper).run(cmd)

        xyz_file = casedir + "/xyz"
        script = f"{exp_home}/offline/Flavour_offline.sh"
        cmd = f"{script} {casedir}/offline {flavour} {xyz_file}"
        logger.info(cmd)
        BatchJob(rte, wrapper=self.wrapper).run(cmd)


class MakeOfflineBinaries(PySurfexBaseTask):
    """Make offline binaries.

    Args:
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct MakeOfflineBinaries task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "MakeOfflineBinaries")

    def execute(self):
        """Execute."""
        rte = os.environ
        wrapper = ""
        casedir = self.platform.get_system_value("casedir")
        exp_home = self.config["platform.deode_home"]
        bindir = f"{self.platform.get_system_value('bindir')}"
        flavour = self.config["compile.build_config"]

        threads = 8
        cmd = (
            f"{exp_home}/offline/Compile_offline.sh {casedir}/offline {flavour} {threads}"
        )
        logger.debug(cmd)
        BatchJob(rte, wrapper=wrapper).run(cmd)

        os.makedirs(bindir, exist_ok=True)
        cmd = (
            f"{exp_home}/offline/Install_offline.sh {casedir}/offline {flavour} {threads}"
        )
        logger.debug(cmd)
        BatchJob(rte, wrapper=wrapper).run(cmd)


class CMakeBuild(PySurfexBaseTask):
    """Make offline binaries.

    Args:
        Task (_type_): _description_

    """

    def __init__(self, config):
        """Construct CMakeBuild task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "CMakeBuild")

    def execute(self):
        """Execute."""
        rte = {**os.environ}
        wrapper = ""

        nproc = 8
        offline_source = self.config["compile.ial_source"]
        cmake_config = self.config["compile.build_config"]
        cmake_config = f"{offline_source}/util/cmake/config/config.{cmake_config}.json"
        if not os.path:
            raise FileNotFoundError(f"CMake config file {cmake_config} not found!")

        casedir = self.platform.get_system_value("casedir")
        bindir = self.platform.get_system_value("bindir")

        build_dir = f"{casedir}/offline/build"
        install_dir = f"{casedir}/offline/install"
        os.makedirs(install_dir, exist_ok=True)
        os.makedirs(build_dir, exist_ok=True)
        prerequisites = ["gribex_370"]
        for project in prerequisites:
            logger.info("Compiling {}", project)
            current_project_dir = f"{offline_source}/util/auxlibs/{project}"
            fproject = project.replace("/", "-")
            current_build_dir = f"{build_dir}/{fproject}"
            logger.info("current build dir ", current_build_dir)
            os.makedirs(current_build_dir, exist_ok=True)
            os.chdir(current_build_dir)
            cmake_flags = "-DCMAKE_BUILD_TYPE=Release "
            cmake_flags += (
                f" -DCMAKE_INSTALL_PREFIX={install_dir} -DCONFIG_FILE={cmake_config} "
            )
            cmd = f"cmake {current_project_dir} {cmake_flags}"
            BatchJob(rte, wrapper=wrapper).run(cmd)
            cmd = f"cmake --build . -j{nproc} --target gribex"
            BatchJob(rte, wrapper=wrapper).run(cmd)
            cmd = "cmake --build . --target install"
            BatchJob(rte, wrapper=wrapper).run(cmd)

        cmake_flags = " -DCMAKE_BUILD_TYPE=Release "
        cmake_flags += f"{cmake_flags} -DCMAKE_INSTALL_PREFIX={install_dir} "
        cmake_flags += f"{cmake_flags} -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=YES "
        cmake_flags += f"{cmake_flags} -DCONFIG_FILE={cmake_config} "

        os.chdir(build_dir)
        # Configure
        cmd = f"cmake {offline_source}/src {cmake_flags}"
        BatchJob(rte, wrapper=wrapper).run(cmd)
        # Build
        targets = "offline-pgd offline-prep offline-offline offline-soda"
        cmd = f"cmake --build . -- -j{nproc} {targets}"
        BatchJob(rte, wrapper=wrapper).run(cmd)

        # Manual installation
        programs = ["PGD-offline", "PREP-offline", "OFFLINE-offline", "SODA-offline"]
        os.makedirs(bindir, exist_ok=True)
        for program in programs:
            logger.info("Installing {}", program)
            shutil.copy(f"{build_dir}/bin/{program}", f"{bindir}/{program}")

        xyz_file = casedir + "/xyz"
        with open(xyz_file, mode="w", encoding="utf-8") as fhandler:
            fhandler.write("-offline")
