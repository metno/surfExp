"""Compilation."""
import os
import shutil

from deode.logs import logger
from pysurfex.run import BatchJob

from surfexp.tasks.tasks import PySurfexBaseTask


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
