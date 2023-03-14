"""Experiment classes and methods."""
import os
import json
import logging
import shutil
import tomlkit
import experiment_scheduler as scheduler
import experiment


class Exp(experiment.Configuration):
    """Experiment class."""

    def __init__(self, exp_dependencies, merged_config, env_system, system_file_paths, server, env_submit, progressObj, domains, stream=None):
        """Instaniate an object of the main experiment class.

        Args:
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration

        """
        logging.debug("Construct Exp")

        # Stream
        merged_config["GENERAL"].update({"STREAM": stream})

        # Scheduler
        merged_config.update({"SCHEDULER": server.settings})
        # Submission
        merged_config.update({"submission": env_submit})
        # System path variables
        merged_config.update({"SYSTEM_FILE_PATHS": system_file_paths})
        # System settings
        merged_config.update({"SYSTEM_VARS": env_system})
        # Date/time
        progress = {
            "DTG": progressObj.dtg_string,
            "DTGEND": progressObj.dtgend_string,
            "DTGBEG": progressObj.dtgbeg_string,
            "DTGPP": progressObj.dtgpp_string
        }
        merged_config.update({"PROGRESS": progress})

        merged_config["GEOMETRY"].update({"DOMAINS": domains})

        # Troika
        troika_config = exp_dependencies["config"]["other_files"]["troika_config.yml"]
        merged_config.update({"TROIKA": {"CONFIG": troika_config}})

        offline_source = exp_dependencies.get("offline_source")
        namelist_dir = exp_dependencies.get("namelist_dir")
        wdir = exp_dependencies.get("exp_dir")
        exp_name = exp_dependencies.get("exp_name")

        self.name = exp_name
        merged_config["GENERAL"].update({"EXP": exp_name})
        merged_config["GENERAL"].update({"EXP_DIR": wdir})
        self.work_dir = wdir
        self.scripts = exp_dependencies.get("pysurfex_experiment")
        self.pysurfex = exp_dependencies.get("pysurfex")
        self.offline_source = offline_source
        merged_config["COMPILE"].update({"OFFLINE_SOURCE": offline_source})
        merged_config["GENERAL"].update({"NAMELIST_DIR": namelist_dir})
        merged_config["GENERAL"].update({"PYSURFEX_EXPERIMENT": self.scripts})
        self.config_file = None
        self.first_guess_yml = exp_dependencies["config"]["other_files"]["first_guess.yml"]
        self.config_yml = exp_dependencies["config"]["other_files"]["config.yml"]

        experiment.Configuration.__init__(self, merged_config)

    def dump_exp_configuration(self, filename, indent=None):
        """Dump the exp configuration.

        The configuration has two keys. One for general config and one for member config.

        Args:
            filename (str): filename to dump to
            indent (int, optional): indentation in json file. Defaults to None.
        """
        json.dump(self.settings, open(filename, mode="w", encoding="utf-8"), indent=indent)
        self.config_file = filename


class ExpFromFiles(Exp):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(self, exp_dependencies, stream=None):
        """Construct an Exp object from files.

        Args:
            exp_dependencies_file (dict): Exp dependencies

        Raises:
            FileNotFoundError: If file is not found

        """
        logging.debug("Construct ExpFromFiles")

        wdir = exp_dependencies.get("exp_dir")
        self.work_dir = wdir

        # System
        env_system = exp_dependencies.get("env_system")
        if os.path.exists(env_system):
            env_system = ExpFromFiles.toml_load(env_system)
        else:
            raise FileNotFoundError("System settings not found " + env_system)

        # System file path
        input_paths = exp_dependencies.get("input_paths")
        if os.path.exists(input_paths):
            with open(input_paths, mode="r", encoding="utf-8") as input_paths:
                system_file_paths = json.load(input_paths)
        else:
            raise FileNotFoundError("System setting input paths not found " + input_paths)

        # Submission settings
        env_submit = exp_dependencies.get("env_submit")
        if os.path.exists(env_submit):
            with open(env_submit, mode="r", encoding="utf-8") as env_submit:
                env_submit = json.load(env_submit)
        else:
            raise FileNotFoundError("Submision settings not found " + env_submit)

        # Scheduler settings
        env_server = exp_dependencies.get("env_server")
        if os.path.exists(env_server):
            server = scheduler.EcflowServerFromFile(env_server)
        else:
            raise FileNotFoundError("Server settings missing " + env_server)

        # Date/time settings
        progressObj = experiment.ProgressFromFiles(wdir, stream=stream)
        progress = {
            "DTG": progressObj.dtg_string,
            "DTGEND": progressObj.dtgend_string,
            "DTGBEG": progressObj.dtgbeg_string,
            "DTGPP": progressObj.dtgpp_string
        }

        # Configuration
        config_files_dict = ExpFromFiles.get_config_files(exp_dependencies["config"]["config_files"],
                                                          exp_dependencies["config"]["blocks"])
        all_merged_settings = self.merge_dict_from_config_dicts(config_files_dict)

        # Stream
        # all_merged_settings["GENERAL"].update({"STREAM": stream})

        # Geometry
        domains = exp_dependencies.get("domains")
        # domains = wdir + "/config/domains/Harmonie_domains.json"
        if os.path.exists(domains):
            with open(domains, mode="r", encoding="utf-8") as domains:
                domains = json.load(domains)
                # all_merged_settings["GEOMETRY"].update({"DOMAINS": domains})
        else:
            raise FileNotFoundError("Domains not found " + domains)

        # Scheduler
        # all_merged_settings.update({"SCHEDULER": server.settings})
        # Submission
        # all_merged_settings.update({"SUBMISSION": env_submit})
        # System path variables
        # all_merged_settings.update({"SYSTEM_FILE_PATHS": system_file_paths})
        # System settings
        # all_merged_settings.update({"SYSTEM_VARS": env_system})
        # Date/time
        # all_merged_settings.update({"PROGRESS": progress})

        # Troika
        # troika_config = exp_dependencies["config"]["other_files"]["troika_config.yml"]
        # all_merged_settings.update({"TROIKA": {"CONFIG": troika_config}})

        Exp.__init__(self, exp_dependencies, all_merged_settings, env_system, system_file_paths,
                     server, env_submit, progressObj, domains, stream=stream)

    @staticmethod
    def toml_load(fname):
        """Load from toml file.

        Using tomlkit to preserve stucture

        Args:
            fname (str): Filename

        Returns:
            _type_: _description_

        """
        f_h = open(fname, "r", encoding="utf-8")
        res = tomlkit.parse(f_h.read())
        f_h.close()
        return res

    @staticmethod
    def toml_dump(to_dump, fname):
        """Dump toml to file.

        Using tomlkit to preserve stucture

        Args:
            to_dump (_type_): _description_
            fname (str): Filename
            mode (str, optional): _description_. Defaults to "w".

        """
        f_h = open(fname, mode="w", encoding="utf-8")
        f_h.write(tomlkit.dumps(to_dump))
        f_h.close()

    @staticmethod
    def merge_dict_from_config_dicts(config_files):
        """Merge the settings in a config dict.

        Args:
            config_files (list): _description_

        Returns:
            _type_: _description_

        """
        logging.debug("config_files: %s", str(config_files))
        merged_env = {}
        for fff in config_files:
            modification = config_files[fff]["toml"]
            merged_env = experiment.Configuration.merge_dict(merged_env, modification)
        return merged_env

    @staticmethod
    def get_config_files(config_files_in, blocks):
        """Get the config files.

        Args:
            config_files (dict): config file and path

        Raises:
            Exception: _description_

        Returns:
            dict: returns a config files dict

        """
        # Check existence of needed config files
        config_files = {}
        for ftype, fname in config_files_in.items():
            if os.path.exists(fname):
                toml_dict = ExpFromFiles.toml_load(fname)
            else:
                raise Exception("No config file found for " + fname)

            config_files.update({
                ftype: {
                    "toml": toml_dict,
                    "blocks": blocks[ftype]["blocks"]
                }
            })
        return config_files

    @staticmethod
    def merge_config_files_dict(config_files, configuration=None, testbed_configuration=None,
                                user_settings=None):
        """Merge config files dicts.

        Args:
            config_files (_type_): _description_
            configuration (_type_, optional): _description_. Defaults to None.
            testbed_configuration (_type_, optional): _description_. Defaults to None.
            user_settings (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_

        """
        logging.debug("Merge config files")
        for this_config_file in config_files:
            logging.debug("This config file %s", this_config_file)
            hm_exp = config_files[this_config_file]["toml"].copy()

            block_config = tomlkit.document()
            if configuration is not None:
                fff = this_config_file.split("/")[-1]
                if fff == "config_exp.toml":
                    block_config.add(tomlkit.comment("\n# SURFEX experiment configuration file\n#"))

            for block in config_files[this_config_file]["blocks"]:
                block_config.update({block: hm_exp[block]})
                if configuration is not None:
                    if block in configuration:
                        merged_config = experiment.Configuration.merge_dict(hm_exp[block], configuration[block])
                        logging.info("Merged: %s %s", block, str(configuration[block]))
                    else:
                        merged_config = hm_exp[block]

                    block_config.update({block: merged_config})

                if testbed_configuration is not None:
                    if block in testbed_configuration:
                        hm_testbed = experiment.Configuration.merge_dict(block_config[block],
                                                                         testbed_configuration[block])
                    else:
                        hm_testbed = block_config[block]
                    block_config.update({block: hm_testbed})

                if user_settings is not None:
                    if not isinstance(user_settings, dict):
                        raise Exception("User settings should be a dict here!")
                    if block in user_settings:
                        logging.info("Merge user settings in block %s", block)
                        user = experiment.Configuration.merge_dict(block_config[block], user_settings[block])
                        block_config.update({block: user})

            logging.debug("block config %s", block_config)
            config_files.update({this_config_file: {"toml": block_config}})
        return config_files

    @staticmethod
    def merge_to_toml_config_files(config_files, wdir, configuration=None, testbed_configuration=None,
                                   user_settings=None,
                                   write_config_files=True):
        """Merge to toml config files.

        Args:
            config_files (_type_): _description_
            wd (_type_): _description_
            configuration (_type_, optional): _description_. Defaults to None.
            testbed_configuration (_type_, optional): _description_. Defaults to None.
            user_settings (_type_, optional): _description_. Defaults to None.
            write_config_files (bool, optional): _description_. Defaults to True.

        """
        config_files = config_files.copy()
        config_files = ExpFromFiles.merge_config_files_dict(config_files, configuration=configuration,
                                                            testbed_configuration=testbed_configuration,
                                                            user_settings=user_settings)

        for fname in config_files:
            this_config_file = f"config/{fname}"

            block_config = config_files[fname]["toml"]
            if write_config_files:
                f_out = f"{wdir}/{this_config_file}"
                dirname = os.path.dirname(f_out)
                dirs = dirname.split("/")
                if len(dirs) > 1:
                    pth = "/"
                    for dname in dirs[1:]:
                        pth = pth + str(dname)
                        os.makedirs(pth, exist_ok=True)
                        pth = pth + "/"
                f_out = open(f_out, mode="w", encoding="utf-8")
                f_out.write(tomlkit.dumps(block_config))
                f_out.close()

    @staticmethod
    def setup_files(wdir, exp_name, host, pysurfex, pysurfex_experiment,
                    offline_source=None, namelist_dir=None, talk=True):
        """Set up the files for an experiment.

        Args:
            wd (_type_): _description_
            exp_name (_type_): _description_
            host (_type_): _description_
            offline_source (_type_, optional): _description_. Defaults to None.
            configuration (_type_, optional): _description_. Defaults to None.
            configuration_file (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_
            Exception: _description_

        """
        exp_dependencies = {}
        if talk:
            logging.info("Setting up for host %s", host)

        # Create needed system files
        if host is None:
            logging.warning("No host specified")
        else:
            system_files = {}
            system_files.update({
                "env_system": "config/system/" + host + ".toml",
                "env": "config/env/" + host + ".py",
                "env_submit": "config/submit/" + host + ".json",
                "env_server": "config/server/" + host + ".json",
                "input_paths": "config/input_paths/" + host + ".json",
            })

            for key, fname in system_files.items():
                lname = f"{wdir}/{fname}"
                gname = f"{pysurfex_experiment}/{fname}"
                if os.path.exists(lname):
                    if talk:
                        logging.info("Using local host specific file %s as %s", lname, key)
                    exp_dependencies.update({key: fname})
                elif os.path.exists(gname):
                    if talk:
                        logging.info("Using general host specific file %s as %s", gname, key)
                    exp_dependencies.update({key: gname})
                else:
                    raise FileNotFoundError(f"No host file found for lname={lname} or gname={gname}")
        
        # Check existence of needed config files
        lconfig = f"{wdir}/config/config.toml"
        gconfig = f"{pysurfex_experiment}/config/config.toml"
        if os.path.exists(lconfig):
            if talk:
                logging.info("Local config definition %s", lconfig)
            config = lconfig
        elif os.path.exists(gconfig):
            if talk:
                logging.info("Global config definition %s", gconfig)
            config = gconfig
        else:
            raise Exception
        c_files = ExpFromFiles.toml_load(config)["config_files"]
        blocks = ExpFromFiles.toml_load(config)
        pysurfex_files = ["config_exp_surfex.toml", "first_guess.yml",
                          "config.yml"]
        c_files = c_files + ["config_exp_surfex.toml"]
        if talk:
            logging.info("Set up toml config files %s", str(c_files))
        cc_files = {}
        for c_f in c_files:
            lname = f"{wdir}/config/{c_f}"
            gname = f"{pysurfex_experiment}/config/{c_f}"
            if c_f in pysurfex_files:
                gname = f"{pysurfex}/surfex/cfg/{c_f}"
            if os.path.exists(lname):
                if talk:
                    logging.info("Using local toml config file %s", lname)
                cc_files.update({c_f: lname})
            elif os.path.exists(gname):
                if talk:
                    logging.info("Using general toml config file %s", gname)
                cc_files.update({c_f: gname})
            else:
                raise FileNotFoundError(f"No toml config file found for lname={lname} or gname={gname}")

        if talk:
            logging.info("Set up other config files %s", str(c_files))
        other_files = {}
        for c_f in ["first_guess.yml", "config.yml", "troika_config.yml"]:
            lname = f"{wdir}/config/{c_f}"
            gname = f"{pysurfex_experiment}/config/{c_f}"
            if c_f in pysurfex_files:
                gname = f"{pysurfex}/surfex/cfg/{c_f}"
            if os.path.exists(lname):
                if talk:
                    logging.info("Using local extra file %s", lname)
                other_files.update({c_f: lname})
            elif os.path.exists(gname):
                if talk:
                    logging.info("Using general extra file %s", gname)
                other_files.update({c_f: gname})
            else:
                raise FileNotFoundError(f"No extra file found for lname={lname} or gname={gname}")

        exp_dependencies.update({"config": {
            "config_files": cc_files,
            "other_files": other_files,
            "blocks": blocks
            }
        })

        ldomains = f"{wdir}/config/domains/Harmonie_domains.json"
        gdomains = f"{pysurfex_experiment}/config/domains/Harmonie_domains.json"
        if os.path.exists(ldomains):
            domains = ldomains
        elif os.path.exists(gdomains):
            domains = gdomains
        else:
            raise Exception

        if namelist_dir is None:
            namelist_dir = f"{pysurfex_experiment}/nam"
            if talk:
                logging.info("Using default namelist directory %s", namelist_dir)

        exp_dependencies.update({"domains": domains})
        exp_dependencies.update({
            "exp_dir": wdir,
            "exp_name": exp_name,
            "pysurfex_experiment": pysurfex_experiment,
            "pysurfex": pysurfex,
            "offline_source": offline_source,
            "namelist_dir": namelist_dir
        })
        return exp_dependencies

    @staticmethod
    def write_exp_config(exp_dependencies, configuration=None, configuration_file=None):
        """Write the exp config to files.

        Args:
            exp_dependencies (dict): _description_
            configuration (_type_, optional): _description_. Defaults to None.
            configuration_file (_type_, optional): _description_. Defaults to None.

        """
        wdir = exp_dependencies["exp_dir"]
        pysurfex_experiment = exp_dependencies["pysurfex_experiment"]
        other_files = exp_dependencies["config"]["other_files"]
        conf = None
        if configuration is not None:
            logging.info("Using configuration %s", configuration)
            lconf = f"{wdir}/config/configurations/{configuration.lower()}.toml"
            gconf = f"{pysurfex_experiment}/config/configurations/{configuration.lower()}.toml"
            if os.path.exists(lconf):
                logging.info("Local configuration file %s", lconf)
                conf = lconf
            elif os.path.exists(gconf):
                logging.info("General configuration file %s", gconf)
                conf = gconf
            else:
                raise Exception

        elif configuration_file is not None:
            logging.info("Using configuration from file %s", configuration_file)
            conf = configuration_file

        if conf is not None:
            if not os.path.exists(conf):
                raise Exception("Can not find configuration " + configuration + " in: " + conf)
            configuration = experiment.ExpFromFiles.toml_load(conf)
        else:
            configuration = None

        # Load config files
        config_files = ExpFromFiles.get_config_files(exp_dependencies["config"]["config_files"],
                                                     exp_dependencies["config"]["blocks"])
        # Merge dicts and write to toml config files
        ExpFromFiles.merge_to_toml_config_files(config_files, wdir, configuration=configuration,
                                                write_config_files=True)

        for ename, extra_file in other_files.items():
            fname = f"config/{ename}"
            if not os.path.exists(fname):
                logging.info("Copy %s to %s", extra_file, fname)
                shutil.copy(extra_file, fname)
            else:
                logging.info("File %s exists", fname)

    @staticmethod
    def dump_exp_dependencies(exp_dependencies, exp_dependencies_file):
        json.dump(exp_dependencies, open(exp_dependencies_file, mode="w", encoding="utf-8"), indent=2)


class ExpFromFilesDepFile(ExpFromFiles):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(self, exp_dependencies_file, stream=None):
        """Construct an Exp object from files.

        Args:
            exp_dependencies_file (str): File with exp dependencies

        Raises:
            FileNotFoundError: If file is not found

        """
        logging.debug("Construct ExpFromFiles")
        if os.path.exists(exp_dependencies_file):
            with open(exp_dependencies_file, mode="r", encoding="utf-8") as exp_dependencies_file:
                exp_dependencies = json.load(exp_dependencies_file)
                ExpFromFiles.__init__(self, exp_dependencies, stream=stream)
        else:
            raise FileNotFoundError("Experiment dependencies not found " + exp_dependencies_file)
