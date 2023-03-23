"""Handle system specific settings."""
import os

import toml

from . import PACKAGE_NAME
from .logs import get_logger


class System:
    """Main system class."""

    def __init__(self, host_system, exp_name, loglevel="INFO"):
        """Constuct a system object.

        Args:
            host_system (dict): Dict describing the system
            exp_name (str): Experiment name.
            loglevel(str, optional): Loglevel. Default to "INFO"

        Raises:
            KeyError: Setting not found

        """
        logger = get_logger(PACKAGE_NAME, loglevel=loglevel)
        logger.debug(str(host_system))
        self.system_variables = [
            "sfx_exp_data",
            "sfx_exp_lib",
            "joboutdir",
            "mkdir",
            "rsync",
            "hosts",
            "troika",
            "sync_data",
            "surfex_config",
        ]
        self.hosts = None
        self.exp_name = exp_name

        # Set system0 from system_dict
        system0 = {}
        for var in self.system_variables:
            if var == "hosts":
                self.hosts = host_system["host_system"]["hosts"]
            elif var == "host":
                pass
            else:
                if var in host_system["host_system"]:
                    system0.update({var: host_system["host_system"][var]})

                # Always sync for HOST0
                elif var == "sync_data" or var == "troika":
                    pass
                else:
                    raise KeyError("Variable is missing: " + var)

        system = {}
        system.update({"hosts": self.hosts})
        for host, host_label in enumerate(self.hosts):
            systemn = system0.copy()
            systemn.update({"host": host_label})
            hostn = "host" + str(host)
            if hostn in host_system["host_system"]:
                for key in host_system["host_system"][hostn]:
                    value = host_system["host_system"][hostn][key]
                    systemn.update({key: value})
            system.update({str(host): systemn})

        self.system = system

    def get_var(self, var, host, stream=None):
        """Get the variable value.

        Args:
            var (_type_): variable name
            host (int): host
            stream (int, optional): _description_. Defaults to None.

        Raises:
            KeyError: variable not found

        Returns:
            any: Variable

        """
        if var == "hosts":
            if self.hosts is not None:
                return self.hosts
            raise KeyError("hosts not found in system")
        if var == "sync_data" and str(host) == "0":
            return None

        if var in self.system[str(host)]:
            if self.system[str(host)][var] is None:
                raise KeyError(var + " is None!")

            if stream is None:
                stream = ""

            value = self.system[str(host)][var]
            if isinstance(value, str):
                value = value.replace("@STREAM@", str(stream))
                value = value.replace("@EXP@", self.exp_name)
                value = value.replace("@USER@", os.environ["USER"])
            return value
        raise KeyError("Variable " + var + " not found in system")


class SystemFromFile(System):
    """Create a system from a toml file."""

    def __init__(self, env_system_file, exp_name, loglevel="INFO"):
        """Construct the System object from a system file.

        Args:
            env_system_file (str): System toml file.
            exp_name (str): Name of the experiment.
            loglevel(str, optional): Loglevel. Default to "INFO"

        Raises:
            FileNotFoundError: If system file not found.

        """
        logger = get_logger(PACKAGE_NAME, loglevel=loglevel)
        logger.debug("Env_system_file: %s", env_system_file)
        if os.path.exists(env_system_file):
            host_system = toml.load(open(env_system_file, mode="r", encoding="utf-8"))
        else:
            raise FileNotFoundError(env_system_file)
        System.__init__(self, host_system, exp_name, loglevel=loglevel)
