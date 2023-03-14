"""Compilation."""
import os
import logging
import shutil
import surfex
from experiment_tasks import AbstractTask


class SyncSourceCode(AbstractTask):
    """Sync source code for offline code.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct SyncSourceCode task.

        Args:
            task (_type_): _description_
            config (_type_): _description_

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""
        rte = os.environ
        wrapper = ""
        rsync = self.config.system.get_var("RSYNC", self.host)
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib") + "/offline"
        os.makedirs(sfx_lib, exist_ok=True)
        offline_source = self.config.get_setting("COMPILE#OFFLINE_SOURCE")
        ifsaux = f"{offline_source}/../../src/ifsaux"
        ifsaux_copy = f"{offline_source}/src/LIB/ifsaux_copy"
        if os.path.exists(ifsaux):
            cmd = f"{rsync} {ifsaux}/* {ifsaux_copy}"
            logging.info(cmd)
            surfex.BatchJob(rte, wrapper=wrapper).run(cmd)
        cmd = f"{rsync} {offline_source}/ {sfx_lib}"
        logging.info(cmd)
        surfex.BatchJob(rte, wrapper=wrapper).run(cmd)

        # Add system files if not existing
        scripts = self.config.get_setting("GENERAL#PYSURFEX_EXPERIMENT")
        host = self.config.system.get_var("SURFEX_CONFIG", self.host)

        system_file_scripts = f"{scripts}/config/offline/conf/system.{host}"
        system_file_lib = f"{sfx_lib}/conf/system.{host}"

        logging.debug("Check system_file_lib %s", system_file_lib)
        if not os.path.exists(system_file_lib):
            logging.debug("Check system_file_scripts %s", system_file_scripts)
            if os.path.exists(system_file_scripts):
                logging.info("Copy %s %s", system_file_scripts, system_file_lib)
                shutil.copy(system_file_scripts, system_file_lib)

        rules_file_scripts = f"{scripts}/config/offline/src/Rules.{host}.mk"
        rules_file_lib = f"{sfx_lib}/src/Rules.{host}.mk"

        logging.debug("Check rules_file_lib %s", rules_file_lib)
        if not os.path.exists(rules_file_lib):
            logging.debug("Check rules_file_scripts %s", rules_file_scripts)
            if os.path.exists(rules_file_scripts):
                logging.info("Copy %s %s", rules_file_scripts, rules_file_lib)
                shutil.copy(rules_file_scripts, rules_file_lib)


class ConfigureOfflineBinaries(AbstractTask):
    """Configure offline binaries.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct ConfigureOfflineBinaries task.

        Args:
            task (_type_): _description_
            config (_type_): _description_

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""
        rte = os.environ
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.surfex_config
        cmd = f"export OFFLINE_CONFIG={flavour} && cd {sfx_lib}/offline/src && " \
              f"./configure OfflineNWP ../conf//system.{flavour}"
        logging.debug(cmd)
        surfex.BatchJob(rte, wrapper=self.wrapper).run(cmd)

        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour
        xyz_file = sfx_lib + "/xyz"
        cmd = ". " + conf_file + "; echo \"$XYZ\" > " + xyz_file
        logging.info(cmd)
        try:
            os.system(cmd)
        except Exception as ex:
            raise Exception("Can not write XYZ ") from ex


class MakeOfflineBinaries(AbstractTask):
    """Make offline binaries.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct MakeOfflineBinaries task.

        Args:
            task (_type_): _description_
            config (_type_): _description_

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""

        rte = {**os.environ}
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.surfex_config

        system_file = sfx_lib + "/offline/conf/system." + flavour
        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour

        cmd = f". {system_file}; . {conf_file}; cd {sfx_lib}/offline/src && make -j 4"
        logging.debug(cmd)
        surfex.BatchJob(rte, wrapper=wrapper).run(cmd)

        os.makedirs(f"{sfx_lib}/offline/exe", exist_ok=True)
        cmd = f". {system_file}; . {conf_file}; cd {sfx_lib}/offline/src && make installmaster"
        logging.debug(cmd)
        surfex.BatchJob(rte, wrapper=wrapper).run(cmd)
