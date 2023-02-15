"""Experiment configuration."""
import os
import json
import shutil
import logging
from datetime import timedelta, datetime
import collections
import copy
import surfex
import experiment_scheduler as scheduler
import experiment


class Configuration():
    """The main experiment configuration.

    It is based on a merged general configuration and a member specific configuration.
    Contrary to the surfex configuration which is a dict with the actual configuration for the
    given member etc.

    """

    def __init__(self, config, sep="#"):
        """Constuct object used to write experiment config files and create suite definition.

        Args:
            config (dict):

        """
        self.sep = sep
        self.config_file = None
        conf_dict, member_conf_dict = self.split_member_settings(config)
        self.sfx_config = surfex.Configuration(conf_dict.copy())
        self.settings = self.sfx_config.settings

        # Find EPS information
        self.members = None
        if "GENERAL" in self.settings:
            if "ENSMSEL" in self.settings["GENERAL"]:
                self.members = self.settings["GENERAL"]["ENSMSEL"]
                if len(self.members) == 0:
                    self.members = None
        self.member_settings = None
        self.task_limit = None
        ensmbr = None
        if "ENSMBR" in self.settings["GENERAL"]:
            ensmbr = self.settings["GENERAL"]["ENSMBR"]
        else:
            self.settings["GENERAL"].update({"ENSMBR": ensmbr})
        self.ensmbr = ensmbr

        member_settings = {}
        # Set EPS config
        if self.members is not None:
            for mbr in self.members:

                if str(mbr) in member_conf_dict:
                    mbr_configs = member_conf_dict[str(mbr)]
                else:
                    raise Exception("Could not find config for member " + str(mbr))
                member_settings.update({str(mbr): mbr_configs})
        self.member_settings = member_settings

        # System variables
        system = self.settings["SYSTEM_VARS"]
        exp_name = None
        exp_dir = None
        if "GENERAL" in self.settings:
            if "EXP" in self.settings["GENERAL"]:
                exp_name = self.settings["GENERAL"]["EXP"]
            if "EXP_DIR" in self.settings["GENERAL"]:
                exp_dir = self.settings["GENERAL"]["EXP_DIR"]
        self.exp_dir = exp_dir
        self.exp_name = exp_name
        if exp_name is not None and exp_dir is not None:
            self.system = experiment.System(system, exp_name)
        else:
            raise Exception("Could not find EXP and/or EXP_DIR")

        # Stream
        stream = None
        if "STREAM" in self.settings:
            stream = self.settings["GENERAL#STREAM"]
        self.stream = stream

        logging.debug("SYSTEM_FILE_PATHS: %s", self.settings["SYSTEM_FILE_PATHS"])
        # System file paths
        system_file_paths = self.settings["SYSTEM_FILE_PATHS"]
        system_file_paths = experiment.SystemFilePathsFromSystem(system_file_paths, self.system,
                                                                 hosts=self.system.hosts,
                                                                 stream=stream, wdir=exp_dir)
        # self.settings.update({"EXP_SYSTEM_FILE_PATHS": self.system_file_paths.paths["0"]})
        # TODO handle host
        self.host = "0"
        self.exp_file_paths = surfex.SystemFilePaths(system_file_paths.paths[self.host])
        self.sfx_exp_vars = {
            "EXP": exp_name,
            "SFX_EXP_LIB": self.exp_file_paths.get_system_path("sfx_exp_lib"),
            "SFX_EXP_DATA": self.exp_file_paths.get_system_path("sfx_exp_data"),
            "SFX_EXP_DIR": exp_dir,
            "PYSURFEX_EXPERIMENT": self.settings["GENERAL"]["PYSURFEX_EXPERIMENT"]
        }
        self.config_file = None

        server = self.settings["SCHEDULER"]
        self.server = scheduler.EcflowServer(ecf_host=server["ECF_HOST"], ecf_port=server["ECF_PORT"])
        self.env_submit = self.settings["SUBMISSION"]

        troika = None
        try:
            troika = self.system.get_var("TROIKA", "0")
        except Exception:
            troika = shutil.which("troika")
        #if troika is None:
        #    raise Exception("Troika not found!")

        troika_config = None
        if "TROIKA" in self.settings:
            if "CONFIG" in self.settings["TROIKA"]:
                troika_config = self.settings["TROIKA"]["CONFIG"]
        if troika_config is None:
            raise Exception("Troika config not found!")
        self.troika = troika
        self.troika_config = troika_config

        if "TASK" not in self.settings:
            self.settings.update({"TASK": {}})
        if "ARGS" not in self.settings["TASK"]:
            self.settings["TASK"].update({"ARGS": {}})

        # Date/time
        progress = self.settings["PROGRESS"]
        self.progress = experiment.ProgressFromDict(progress)

        ##################################################################################
        # Update time information                     Is this needed????
        cycle_times = self.get_cycle_list()
        fcint = {}
        fgint = {}
        for cycle in cycle_times:
            cycle_delta = timedelta(hours=int(cycle))
            cycle_string = self.format_cycle_string(cycle_delta)
            fcint.update({cycle_string: int(self.set_fcint(cycle_delta).total_seconds())})
            fgint.update({cycle_string: int(self.set_fgint(cycle_delta).total_seconds())})
        logging.debug("fcint %s", str(fcint))
        logging.debug("fgint %s", str(fgint))
        self.settings["GENERAL"].update({"FCINT": fcint})
        self.settings["GENERAL"].update({"FGINT": fgint})
        if self.members is not None:
            for mbr in self.members:
                fcint_members = {}
                fgint_members = {}
                cycle_times = self.get_cycle_list()
                for cycle in cycle_times:
                    cycle_delta = timedelta(hours=int(cycle))
                    cycle_string = self.format_cycle_string(cycle_delta)
                    secs = int(self.set_fcint(cycle_delta).total_seconds())
                    fcint_members.update({cycle_string: secs})
                    secs = int(self.set_fgint(cycle_delta).total_seconds())
                    fgint_members.update({cycle_string: secs})
                self.member_settings[str(mbr)]["GENERAL"].update({"FCINT": fcint_members})
                self.member_settings[str(mbr)]["GENERAL"].update({"FGINT": fgint_members})
                logging.debug("fcint_members %s", str(fcint_members))
                logging.debug("fgint_members %s", str(fgint_members))

    def get_nnco(self, dtg=None):
        """Get the active observations.

        Args:
            dtg (_type_, optional): datetime.datetime. Defaults to None.

        Returns:
            list: List with either 0 or 1

        """
        # Some relevant assimilation settings
        obs_types = self.settings["SURFEX"]["ASSIM"]["OBS"]["COBS_M"]
        nnco_r = self.settings["SURFEX"]["ASSIM"]["OBS"]["NNCO"]
        snow_ass = self.settings["SURFEX"]["ASSIM"]["ISBA"]["UPDATE_SNOW_CYCLES"]
        snow_ass_done = False
        if dtg is None:
            dtg = self.progress.dtg
        if len(snow_ass) > 0:
            if dtg is not None:
                hhh = int(dtg.strftime("%H"))
                for s_n in snow_ass:
                    if hhh == int(s_n):
                        snow_ass_done = True
        nnco = []
        for ivar, __ in enumerate(obs_types):
            ival = 0
            if nnco_r[ivar] == 1:
                ival = 1
                if obs_types[ivar] == "SWE":
                    if not snow_ass_done:
                        logging.info("Disabling snow assimilation since cycle is not in %s", snow_ass)
                        ival = 0
            logging.debug("ivar=%s ival=%s", ivar, ival)
            nnco.append(ival)

        logging.debug("NNCO: %s", nnco)
        return nnco

    def split_member_settings(self, merged_settings):
        """Process the settings and split out member settings.

        Args:
            merged_settings (dict): dict with all settings

        Returns:
            (dict, dict): General config, member_config

        """
        # Write member settings
        members = None
        if "GENERAL" in merged_settings:
            if "ENSMSEL" in merged_settings["GENERAL"]:
                members = list(merged_settings["GENERAL"]["ENSMSEL"])

        logging.debug("Merged settings %s", merged_settings)
        merged_member_settings = {}
        if "EPS" in merged_settings:
            if "MEMBER_SETTINGS" in merged_settings["EPS"]:
                merged_member_settings = copy.deepcopy(merged_settings["EPS"]["MEMBER_SETTINGS"])
                logging.debug("Found member settings %s", merged_member_settings)
                del merged_settings["EPS"]["MEMBER_SETTINGS"]

        member_settings = {}
        if members is not None:
            for mbr in members:
                toml_settings = copy.deepcopy(merged_settings)
                logging.debug("member: %s merged_member_dict: %s", str(mbr), merged_member_settings)
                member_dict = self.get_member_settings(merged_member_settings, mbr)
                logging.debug("member_dict: %s", member_dict)
                toml_settings = Configuration.merge_dict(toml_settings, member_dict)
                member_settings.update({str(mbr): toml_settings})

        return merged_settings, member_settings

    @staticmethod
    def deep_update(source, overrides):
        """Update a nested dictionary or similar mapping.

        Modify ``source`` in place.

        Args:
            source (_type_): _description_
            overrides (_type_): _description_

        Returns:
            _type_: _description_

        """
        for key, value in overrides.items():
            if isinstance(value, collections.Mapping) and value:
                returned = Configuration.deep_update(source.get(key, {}), value)
                source[key] = returned
            else:
                override = overrides[key]

                source[key] = override

        return source

    @staticmethod
    def merge_dict(old_env, mods):
        """Merge the dicts from toml by a deep update.

        Args:
            old_env (_type_): _description_
            mods (_type_): _description_

        Returns:
            _type_: _description_

        """
        return Configuration.deep_update(old_env, mods)

    @staticmethod
    def flatten(ddd, sep="#"):
        """Flatten the setting to get member setting.

        Args:
            d (_type_): _description_
            sep (str, optional): Separator. Defaults to "#".

        Returns:
            _type_: _description_

        """
        obj = collections.OrderedDict()

        def recurse(ttt, parent_key=""):
            if isinstance(ttt, list):
                for i in enumerate(ttt):
                    recurse(ttt[i], parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(ttt, dict):
                for k, vvv in ttt.items():
                    recurse(vvv, parent_key + sep + k if parent_key else k)
            else:
                obj[parent_key] = ttt

        recurse(ddd)
        return obj

    def get_member_settings(self, ddd, member):
        """Get the member setting.

        Args:
            d (_type_): _description_
            member (_type_): _description_
            sep (str, optional): _description_. Defaults to "#".

        Returns:
            _type_: _description_

        """

        member_settings = {}
        settings = Configuration.flatten(ddd)
        for setting in settings:
            keys = setting.split(self.sep)
            logging.debug("Keys: %s", str(keys))
            if len(keys) == 1:
                member3 = f"{int(member):03d}"
                val = settings[setting]
                if isinstance(val, str):
                    val = val.replace("@EEE@", member3)

                this_setting = {keys[0]: val}
                member_settings = Configuration.merge_dict(member_settings, this_setting)
            else:
                this_member = int(keys[-1])
                keys = keys[:-1]
                logging.debug("This member: %s member=%s Keys=%s", str(this_member), str(member), keys)
                if int(this_member) == int(member):
                    this_setting = settings[setting]
                    for key in reversed(keys):
                        this_setting = {key: this_setting}

                    logging.debug("This setting: %s", str(this_setting))
                    member_settings = Configuration.merge_dict(member_settings, this_setting)
                    logging.debug("Merged member settings for member %s = %s",
                                  str(member), str(member_settings))
        logging.debug("Finished member settings for member %s = %s", str(member), str(member_settings))
        return member_settings

    @staticmethod
    def format_cycle_string(cycle_delta):
        """Format the cycle string.

        Args:
            cycle_delta (datetime.timedelta): The time delta from midnight.

        Returns:
            str: Formatted string
        """
        secs = cycle_delta.total_seconds()
        hours = int(secs / 3600)
        mins = int(secs % 3600)
        return f"{hours:02}{mins:02}"

    def dump_json(self, filename, indent=None):
        """Dump a json file with configuration.

        Args:
            filename (str): Filename of json file to write
            indent (int): Indentation in filename

        Returns:
            None

        """
        if json is None:
            raise Exception("json module not loaded")

        if self.members is not None:
            settings = self.member_settings.copy()
        else:
            settings = self.settings.copy()

        logging.debug("Settings: %s", str(settings))
        with open(filename, mode="w", encoding="UTF-8") as file_handler:
            json.dump(settings, file_handler, indent=indent)

    def max_fc_length(self):
        """Calculate the max forecast time.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            _type_: _description_
        """
        ll_list = self.get_lead_time_list()
        max_fc = -1
        for ll_val in ll_list:
            if int(ll_val) > int(max_fc):
                max_fc = int(ll_val)
        return max_fc

    def setting_is(self, setting, value, **kwargs):
        """Check if setting is value.

        Args:
            setting (_type_): _description_
            value (_type_): _description_

        Returns:
            bool: True if found, False if not found.
        """
        if self.get_setting(setting, **kwargs) == value:
            return True
        return False

    def setting_is_not(self, setting, value, **kwargs):
        """Check if setting is not value.

        Args:
            setting (_type_): _description_
            value (_type_): _description_

        Returns:
            bool: True if not found, False if found.

        """
        found = False
        if self.get_setting(setting, **kwargs) == value:
            found = True

        if found:
            return False
        return True

    def value_is_one_of(self, setting, value, **kwargs):
        """Check if the setting contains value.

        Args:
            setting (str): _description_
            value (list): _description_

        Returns:
            bool: True if found, False if not found.

        """
        found = False
        setting = self.get_setting(setting, **kwargs)
        for test_setting in setting:
            if test_setting == value:
                found = True
        return found

    def value_is_not_one_of(self, setting, value, **kwargs):
        """Check if the setting does not contain value.

        Args:
            setting (_type_): _description_
            value (list): _description_

        Returns:
            bool: True if not found, False if found.

        """
        found = self.value_is_one_of(setting, value, **kwargs)
        if found:
            return False
        return True

    def setting_is_one_of(self, setting, values, **kwargs):
        """Check if setting is one of the provided list of values.

        Args:
            setting (_type_): _description_
            values (_type_): _description_

        Raises:
            Exception: _description_

        Returns:
            bool: True if found, False if not found.

        """
        found = False
        setting = self.get_setting(setting, **kwargs)
        if not isinstance(values, list):
            raise Exception("Excpected a list as input, got ", type(values))
        for val in values:
            if setting == val:
                found = True
        return found

    def setting_is_not_one_of(self, setting, values, **kwargs):
        """Check if setting is not one of the provided list of values.

        Args:
            setting (_type_): _description_
            values (_type_): _description_

        Returns:
            bool: True if not found, False if found.

        """
        found = self.setting_is_one_of(setting, values, **kwargs)
        if found:
            return False
        return True

    def get_setting(self, setting, check_parsing=True, validtime=None, basedtg=None,
                    tstep=None, pert=None, var=None, default=None, abort=False):
        """Get setting.

        Args:
            setting (_type_): _description_
            check_parsing (bool, optional): _description_. Defaults to True.
            validtime (_type_, optional): _description_. Defaults to None.
            basedtg (_type_, optional): _description_. Defaults to None.
            tstep (_type_, optional): _description_. Defaults to None.
            pert (_type_, optional): _description_. Defaults to None.
            var (_type_, optional): _description_. Defaults to None.
            default (_type_, optional): _description_. Defaults to None.
            abort (bool, optional): _description_. Defaults to True.

        Returns:
            _type_: _description_

        """
        this_setting = self.sfx_config.get_setting(setting, check_parsing=False, validtime=validtime,
                                                   basedtg=basedtg, tstep=tstep, pert=pert, var=var,
                                                   default=default, abort=abort)

        # Parse setting
        this_setting = self.parse_setting(this_setting, check_parsing=check_parsing,
                                          validtime=validtime, basedtg=basedtg, tstep=tstep,
                                          pert=pert, var=var)
        return this_setting

    def parse_setting(self, setting, check_parsing=True, validtime=None, basedtg=None, tstep=None,
                      pert=None, var=None):
        """Parse the setting.

        Args:
            setting (_type_): _description_
            check_parsing (bool, optional): _description_. Defaults to True.
            validtime (_type_, optional): _description_. Defaults to None.
            basedtg (_type_, optional): _description_. Defaults to None.
            tstep (_type_, optional): _description_. Defaults to None.
            pert (_type_, optional): _description_. Defaults to None.
            var (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_

        """
        # Check on arguments
        if isinstance(setting, str):

            if basedtg is not None:
                if isinstance(basedtg, str):
                    basedtg = datetime.strptime(basedtg, "%Y%m%d%H")
            if validtime is not None:
                if isinstance(validtime, str):
                    validtime = datetime.strptime(validtime, "%Y%m%d%H")
            else:
                validtime = basedtg

            if basedtg is not None and validtime is not None:
                lead_time = validtime - basedtg
                setting = str(setting).replace("@YYYY_LL@", validtime.strftime("%Y"))
                setting = str(setting).replace("@MM_LL@", validtime.strftime("%m"))
                setting = str(setting).replace("@DD_LL@", validtime.strftime("%d"))
                setting = str(setting).replace("@HH_LL@", validtime.strftime("%H"))
                setting = str(setting).replace("@mm_LL@", validtime.strftime("%M"))
                lead_seconds = int(lead_time.total_seconds())
                # lead_minutes = int(lead_seconds / 3600)
                lead_hours = int(lead_seconds / 3600)
                setting = str(setting).replace("@LL@", f"{lead_hours:02d}")
                setting = str(setting).replace("@LLL@", f"{lead_hours:03d}")
                setting = str(setting).replace("@LLLL@", f"{lead_hours:04d}")
                if tstep is not None:
                    lead_step = int(lead_seconds / tstep)
                    setting = str(setting).replace("@TTT@", f"{lead_step:03d}")
                    setting = str(setting).replace("@TTTT@", f"{lead_step:04d}")

            if basedtg is not None:
                setting = str(setting).replace("@YMD@", basedtg.strftime("%Y%m%d"))
                setting = str(setting).replace("@YYYY@", basedtg.strftime("%Y"))
                setting = str(setting).replace("@YY@", basedtg.strftime("%y"))
                setting = str(setting).replace("@MM@", basedtg.strftime("%m"))
                setting = str(setting).replace("@DD@", basedtg.strftime("%d"))
                setting = str(setting).replace("@HH@", basedtg.strftime("%H"))
                setting = str(setting).replace("@mm@", basedtg.strftime("%M"))

            if self.ensmbr is not None:
                setting = str(setting).replace("@E@", f"mbr{int(self.ensmbr):d}")
                setting = str(setting).replace("@EE@", f"mbr{int(self.ensmbr):02d}")
                setting = str(setting).replace("@EEE@", f"mbr{int(self.ensmbr):03d}")
            else:
                setting = str(setting).replace("@E@", "")
                setting = str(setting).replace("@EE@", "")
                setting = str(setting).replace("@EEE@", "")

            if pert is not None:
                logging.debug("replace %s in %s", pert, setting)
                setting = str(setting).replace("@PERT@", str(pert))
                logging.debug("replaced %s in %s", pert, setting)

            if var is not None:
                setting = str(setting).replace("@VAR@", var)

            if self.sfx_exp_vars is not None:
                logging.debug("%s %s", str(self.sfx_exp_vars), str(setting))
                for sfx_exp_var, __ in self.sfx_exp_vars.items():
                    if isinstance(self.sfx_exp_vars[sfx_exp_var], str):
                        logging.debug("%s  <--> %s @%s@", str(setting), str(sfx_exp_var),
                                      str(self.sfx_exp_vars[sfx_exp_var]))
                        setting = str(setting).replace("@" + sfx_exp_var + "@",
                                                       self.sfx_exp_vars[sfx_exp_var])

        if check_parsing:
            if isinstance(setting, str) and setting.count("@") > 1:
                raise Exception("Setting was not substituted properly? " + setting)

        return setting

    def update_setting(self, setting, value, sep="#"):
        """Update the setting.

        Args:
            setting (_type_): _description_
            value (_type_): _description_
            mbr (_type_, optional): _description_. Defaults to None.
            sep (str, optional): _description_. Defaults to "#".

        Raises:
            Exception: _description_

        """
        if sep is None:
            keys = [setting]
        else:
            keys = setting.split(sep)

        last_key = keys[-1]
        dsetting = {last_key: value}
        if len(keys) > 1:
            for key in reversed(keys[0:-1]):
                dsetting = {key: dsetting}

        mbr = self.ensmbr
        if mbr is None:
            self.settings = self.merge_dict(self.settings, dsetting)
        else:
            if self.members is not None and str(mbr) in self.members:
                self.member_settings[str(mbr)] = \
                    self.merge_dict(self.member_settings[str(mbr)], dsetting)
            else:
                raise Exception("Not a valid member: " + str(mbr))

    def get_total_unique_cycle_list(self):
        """Get a list of unique start times for the forecasts.

        Returns:
            list: List with times
        """
        # Create a list of all unique HHs from all members
        hh_list_all = []
        if self.members is not None:
            for __ in self.members:
                hh_l = self.get_cycle_list()
                for hour in hh_l:
                    hour = f"{int(hour):02d}"
                    if hour not in hh_list_all:
                        hh_list_all.append(hour)
        else:
            hh_l = self.get_cycle_list()
            for hour in hh_l:
                hour = f"{int(hour):02d}"
                if hour not in hh_list_all:
                    hh_list_all.append(hour)

        # Sort this list
        hh_list = []
        for hour in sorted(hh_list_all):
            hh_list.append(hour)

        return hh_list

    def get_fgint(self, dtg):
        """Get the fgint.

        Args:
            dtg (datetime.datetime): Cycle time.
            mbr (int, optional): Ensemble member. Defaults to None.

        Returns:
            int: fgint in seconds

        """
        time_stamp = dtg.strftime("%H%M")
        if "FGINT" in self.settings["GENERAL"]:
            fgints = self.settings["GENERAL"]["FGINT"]
        else:
            raise Exception("FGINT not found")
        fgint = fgints.get(time_stamp)
        logging.debug("FGINT=%s DTG=%s fgints=%s time_stamp=%s", fgint, dtg, str(fgints),
                      time_stamp)
        return fgint

    def get_fcint(self, dtg):
        """Get the fcint.

        Args:
            dtg (datetime.datetime): Cycle time.
            mbr (int, optional): Ensemble member. Defaults to None.

        Returns:
            int: fcint in seconds

        """
        time_stamp = dtg.strftime("%H%M")
        if "FCINT" in self.settings["GENERAL"]:
            fcints = self.settings["GENERAL"]["FCINT"]
        else:
            raise Exception("FCINT not found")
        fcint = fcints.get(time_stamp)
        logging.debug("FCINT=%s DTG=%s fcints=%s time_stamp=%s", fcint, dtg, str(fcints),
                      time_stamp)
        return fcint

    def set_fgint(self, cycle):
        """Set the the interval between the forecasts before.

        Args:
            cycle (datetime.timedelta): Timedelta from midnight
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            datetime.timedelta: First guess intervall

        """
        hh_list = self.get_cycle_list()
        prev_delta = None
        for delta, val in enumerate(hh_list):
            val = timedelta(hours=int(val))
            logging.debug("delta=%s val=%s cycle=%s", delta, val, cycle)
            if val == cycle:
                if prev_delta is not None:
                    return val - prev_delta
            prev_delta = val
        # 24 hours timdelta if we only have one cycle pr day.
        if len(hh_list) == 1:
            prev_delta = timedelta(hours=0)
        return timedelta(hours=24) - prev_delta

    def set_fcint(self, cycle):
        """Set the the interval between the forecasts after.

        Args:
            cycle (datetime.timedelta): Timedelta from midnight
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            datetime.timedelta: Interval to  next forecast.

        """
        hh_list = self.get_cycle_list()
        start_delta = None
        first_val = None
        for delta, val in enumerate(hh_list):
            val = timedelta(hours=int(val))
            logging.debug("delta=%s val=%s cycle=%s", delta, val, cycle)
            if start_delta is not None:
                return val - start_delta
            if val == cycle:
                start_delta = val
            if first_val is None:
                first_val = val

        # 24 hours timdelta if we only have one cycle pr day.
        if len(hh_list) == 1:
            start_delta = timedelta(hours=0)
        return timedelta(hours=24) - start_delta

    def get_hh_list(self):
        """_summary_

        Raises:
            Exception: _description_
        """
        if "HH_LIST" in self.settings["GENERAL"]:
            return self.settings["GENERAL"]["HH_LIST"]
        else:
            raise Exception("HH_LIST not found")

    def get_ll_list(self):
        """_summary_

        Raises:
            Exception: _description_
        """
        if "LL_LIST" in self.settings["GENERAL"]:
            return self.settings["GENERAL"]["LL_LIST"]
        else:
            raise Exception("LL_LIST not found")

    def get_cycle_list(self):
        """Get a list of forecast start times.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            list: Forecast start times.

        """
        hh_list, __ = self.expand_hh_and_ll_list()
        logging.debug("hh_list: %s", hh_list)
        return hh_list

    def get_lead_time_list(self):
        """Get a list of forecast lead times.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            list: Forecast lead times.

        """
        __, ll_list = self.expand_hh_and_ll_list()
        logging.debug("ll_list: %s", ll_list)
        return ll_list

    @staticmethod
    def expand_list(string, fmt="{:03d}", sep1=",", sep2=":", sep3="-", maxval=None, add_last=False,
                    tstep=None):
        """Expand the lists of forecast start times/lead times.

        Args:
            string (str): _description_
            fmt (str, optional): format. Defaults to "{:03d}".
            sep1 (str, optional): First separator. Defaults to ",".
            sep2 (str, optional): Second separator. Defaults to ":".
            sep3 (str, optional): Third separator. Defaults to "-".
            maxval (int, optional): Maximum value. Defaults to None.
            add_last (bool, optional): if last value should be added. Defaults to False.
            tstep (int, optional): time step in forecast. Needed for minute output.
                                   Defaults to None.

        Raises:
            Exception: _description_
            Exception: _description_
            Exception: _description_

        Returns:
            list: Expaned list of values.

        """
        elements = string.split(sep1)
        expanded_list = []
        if string.strip() == "":
            return expanded_list

        for __, element in enumerate(elements):
            if element.find(sep2) > 0 or element.find(sep3) > 0:
                step = 1
                if element.find(sep2) > 0:
                    p_1, step = element.split(sep2)
                else:
                    p_1 = element

                start, end = p_1.split(sep3)
                for ltime in range(int(start), int(end) + 1, int(step)):
                    add = True
                    if maxval is not None:
                        if ltime > maxval:
                            add = False
                    if add:
                        if tstep is not None:
                            if (ltime * 60) % tstep == 0:
                                ltime = int(ltime * 60 / tstep)
                            else:
                                logging.critical("Time step %s tstep=%s", str(ltime * tstep),
                                                 str(tstep))
                                raise Exception("Time step is not a minute!")
                        this_ll = fmt.format(ltime)
                        expanded_list.append(this_ll)
            else:
                add = True
                ltime = int(element)
                if maxval is not None:
                    if ltime > maxval:
                        add = False
                if add:
                    if tstep is not None:
                        if (ltime * 60) % tstep == 0:
                            ltime = int(ltime * 60 / tstep)
                        else:
                            raise Exception("Time step is not a minute! " + str(ltime))
                    ltime = fmt.format(ltime)
                    expanded_list.append(ltime)

        # Add last value if wanted and not existing
        if maxval is not None and add_last:
            if tstep is not None:
                if (maxval * 60) % tstep == 0:
                    maxval = int(maxval * 60 / tstep)
                else:
                    raise Exception("Time step is not a minute!")
            if str(maxval) not in expanded_list:
                ltime = fmt.format(maxval)
                expanded_list.append(ltime)
        return expanded_list

    def expand_hh_and_ll_list(self, sep=":"):
        """Expand both HH_LIST and LL_LIST.

        Args:
            hh_list (_type_): _description_
            ll_list (_type_): _description_
            sep (str, optional): _description_. Defaults to ":".

        Raises:
            Exception: _description_

        Returns:
            tuple: expanded_hh_list, expanded_ll_list

        """
        hhs = self.expand_list(self.get_hh_list(), fmt="{:02d}")
        lls_in = self.expand_list(self.get_ll_list(), fmt="{:d}")

        lls = []
        j = 0
        for __ in range(0, len(hhs)):
            lls.append(lls_in[j])
            j = j + 1
            if j == len(lls_in):
                j = 0

        if len(hhs) != len(lls):
            raise Exception
        expanded_hh_list = hhs
        expanded_ll_list = lls
        #expanded_hh_list = []
        #expanded_ll_list = []
        #for __, lll in enumerate(hhs):
        #    logging.debug("Hour: %s", lll)
        #    if lll.find(sep) > 0:
        #        p_1, step = lll.split(sep)
        #        h_1, h_2 = p_1.split("-")
        #        for hour in range(int(h_1), int(h_2) + 1, int(step)):
        #            hour = f"{hour:02d}"
        #            expanded_hh_list.append(hour)
        #            expanded_ll_list.append(lll)
        #    else:
        #        hour = f"{int(lll):02d}"
        #        expanded_hh_list.append(hour)
        #        expanded_ll_list.append(lll)
        logging.debug("Expanded hh list=%s, expanded ll list=%s",
                      expanded_hh_list, expanded_ll_list)
        return expanded_hh_list, expanded_ll_list


class ConfigurationFromJsonFile(Configuration):
    """Create an experiment configuration from a json file."""

    def __init__(self, filename):
        """Create an experiment configuration from a json file.

        Args:
            filename (str): Filename

        """
        with open(filename, mode="r", encoding="utf-8") as file_handler:
            all_settings = json.load(file_handler)
        Configuration.__init__(self, all_settings)
        self.config_file = os.path.abspath(filename)
