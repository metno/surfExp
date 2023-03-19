"""Handle system specific settings."""
import os
import logging
import json
import toml


class System():
    """Main system class."""

    def __init__(self, host_system, exp_name):
        """Constuct a system object.

        Args:
            host_system (dict): Dict describing the system
            exp_name (str): Experiment name.

        Raises:
            Exception: _description_
        """
        logging.debug(str(host_system))
        self.system_variables = ["sfx_exp_data", "sfx_exp_lib", "joboutdir", "mkdir",
                                 "rsync", "hosts", "troika",
                                 "sync_data", "surfex_config"]
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
                    raise Exception("Variable is missing: " + var)

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

    def dump_system_vars(self, filename, indent=None, stream=None):
        """Dump system variables to a json file.

        Args:
            filename (_type_): _description_
            indent (_type_, optional): _description_. Defaults to None.
            stream (_type_, optional): _description_. Defaults to None.

        """
        system_vars = {}
        for host in range(0, len(self.hosts)):
            host = str(host)
            var_host = {}
            for key in self.system_variables:
                if key == "hosts":
                    value = self.get_var(key, host, stream=stream)
                    var_host.update({"hostname": value[int(host)]})
                else:
                    value = self.get_var(key, host, stream=stream)
                    var_host.update({key: value})
            logging.debug("HOST=%s KEY=%s VALUE=%s", str(host), str(key), str(value))
            system_vars.update({host: var_host})
        json.dump(system_vars, open(filename, mode="w", encoding="utf-8"), indent=indent)

    def get_var(self, var, host, stream=None):
        """Get the variable value.

        Args:
            var (_type_): variable name
            host (int): host
            stream (int, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_
            Exception: _description_
            Exception: _description_

        Returns:
            _type_: variable value.

        """
        if var == "hosts":
            if self.hosts is not None:
                return self.hosts
            raise Exception("hosts not found in system")
        if var == "sync_data" and str(host) == "0":
            return None

        if var in self.system[str(host)]:
            if self.system[str(host)][var] is None:
                raise Exception(var + " is None!")

            if stream is None:
                stream = ""

            value = self.system[str(host)][var]
            if isinstance(value, str):
                value = value.replace("@STREAM@", str(stream))
                value = value.replace("@EXP@", self.exp_name)
                value = value.replace("@USER@", os.environ["USER"])
            return value
        raise Exception("Variable " + var + " not found in system")


class SystemFromFile(System):
    """Create a system from a toml file."""

    def __init__(self, env_system_file, exp_name):
        """Construct the System object from a system file.

        Args:
            env_system_file (str): System toml file.
            exp_name (str): Name of the experiment.

        Raises:
            FileNotFoundError: If system file not found.

        """
        logging.debug("Env_system_file: %s", env_system_file)
        if os.path.exists(env_system_file):
            host_system = toml.load(open(env_system_file, mode="r", encoding="utf-8"))
        else:
            raise FileNotFoundError(env_system_file)
        System.__init__(self, host_system, exp_name)


'''
class SystemFilePathsFromSystem():
    """Set system file paths from a system object.

    Also set SFX_EXP system variables (File stucture/ssh etc)

    """

    def __init__(self, paths_in, system, hosts=None, stream=None, wdir=None):
        """Construct a SystemFilePathsFromSystem object.

        Args:
            paths_in (_type_): _description_
            system (_type_): _description_
            hosts (_type_, optional): _description_. Defaults to None.
            stream (_type_, optional): _description_. Defaults to None.
            wdir (_type_, optional): _description_. Defaults to None.

        """
        # surfex.SystemFilePaths.__init__(self, paths)
        if hosts is None:
            hosts = ["0"]

        # override paths from system file
        paths = {}
        for host in range(0, len(hosts)):
            host = str(host)
            paths_host = {}
            paths_host.update(paths_in)
            if wdir is not None:
                paths_host.update({"exp_dir": wdir})

            sfx_data = system.get_var("sfx_exp_data", host=host, stream=stream)
            sfx_lib = system.get_var("sfx_exp_lib", host=host, stream=stream)

            default_bin_dir = sfx_data + "/lib/offline/exe/"
            default_clim_dir = sfx_data + "/climate/"
            default_archive_dir = sfx_data + "/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@/"
            default_first_guess_dir = default_archive_dir
            default_extarch_dir = sfx_data + "/archive/extract/"
            default_forcing_dir = sfx_data + "/forcing/@YYYY@@MM@@DD@@HH@/@EEE@/"
            default_obs_dir = sfx_data + "/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@/"
            wrk_dir = sfx_data + "/@YYYY@@MM@@DD@_@HH@/@EEE@/"
            paths_host.update({
                "sfx_exp_data": sfx_data,
                "sfx_exp_lib": sfx_lib,
                "default_bin_dir": default_bin_dir,
                "default_archive_dir": default_archive_dir,
                "default_first_guess_dir": default_first_guess_dir,
                "default_extrarch_dir": default_extarch_dir,
                "default_climdir": default_clim_dir,
                "default_wrk_dir": wrk_dir,
                "default_forcing_dir": default_forcing_dir,
                "default_pgd_dir": default_clim_dir,
                "default_prep_dir": default_archive_dir,
                "default_obs_dir": default_obs_dir
            })
            paths.update({host: paths_host})
        self.paths = paths

    def dump_system(self, filename, indent=None):
        """Dump the system to a json file.

        Args:
            filename (str): filename
            indent (int, optional): indentation in file. Defaults to None.
        """
        json.dump(self.paths, open(filename, mode="w", encoding="utf-8"), indent=indent)


class SystemFilePathsFromSystemFile(SystemFilePathsFromSystem):
    """Set systemfilepaths from a system file.

    Also set SFX_EXP system variables (File stucture/ssh etc)

    """

    def __init__(self, system_file_paths, system, name, hosts=None, stream=None, wdir=None):
        """Construct the SystemFilePathsFromSystem object.

        From a systemfilepath file and a system file.

        Args:
            system_file_paths (_type_): _description_
            system (_type_): _description_
            name (_type_): _description_
            hosts (_type_, optional): _description_. Defaults to None.
            stream (_type_, optional): _description_. Defaults to None.
            wdir (_type_, optional): _description_. Defaults to None.
        """
        system_file_paths = json.load(open(system_file_paths, mode="r", encoding="UTF-8"))
        system = SystemFromFile(system, name)
        SystemFilePathsFromSystem.__init__(self, system_file_paths, system, hosts=hosts,
                                           stream=stream, wdir=wdir)
'''
