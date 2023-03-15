"""Forcing task."""
import os
from datetime import timedelta
import json
import logging
import yaml


import surfex


from ..tasks.tasks import AbstractTask


class Forcing(AbstractTask):
    """Create forcing task."""

    def __init__(self, config):
        """Construct forcing task.

        Args:
            config (dict): Actual configuration dict

        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME")
        user_config = None
        # TODO fix this test
        if "TASK" in self.config.settings:
            if "FORCING_USER_CONFIG" in self.config.settings["TASK"]:
                user_config = self.config.get_setting("TASK#FORCING_USER_CONFIG", default=None)
        self.user_config = user_config

    def execute(self):
        """Execute the forcing task.

        Raises:
            NotImplementedError: _description_
        """
        dtg = self.dtg
        fcint = self.fcint

        kwargs = {}
        if self.user_config is not None:
            user_config = yaml.safe_load(open(self.user_config, mode="r", encoding="utf-8"))
            kwargs.update({"user_config": user_config})

        with open(self.wdir + "/domain.json", mode="w", encoding="utf-8") as file_handler:
            json.dump(self.geo.json, file_handler, indent=2)
        kwargs.update({"domain": self.wdir + "/domain.json"})
        # global_config = self.work_dir + "/config/config.yml"
        global_config = self.config.config_yml
        with open(global_config, mode="r", encoding="utf-8") as file_handler:
            global_config = yaml.safe_load(file_handler)
        kwargs.update({"config": global_config})

        kwargs.update({"dtg_start": dtg.strftime("%Y%m%d%H")})
        kwargs.update({"dtg_stop": (dtg + timedelta(seconds=fcint)).strftime("%Y%m%d%H")})

        forcing_dir = self.exp_file_paths.get_system_path("forcing_dir", basedtg=self.dtg,
                                                          default_dir="default_forcing_dir")
        os.makedirs(forcing_dir, exist_ok=True)

        output_format = self.config.get_setting("SURFEX#IO#CFORCING_FILETYPE").lower()
        if output_format == "netcdf":
            output = forcing_dir + "/FORCING.nc"
        else:
            raise NotImplementedError(output_format)

        kwargs.update({"of": output})
        kwargs.update({"output_format": output_format})

        pattern = self.config.get_setting("FORCING#PATTERN", check_parsing=False)
        input_format = self.config.get_setting("FORCING#INPUT_FORMAT")
        kwargs.update({"geo_input_file": self.config.get_setting("FORCING#INPUT_GEO_FILE")})
        zref = self.config.get_setting("FORCING#ZREF")
        zval = self.config.get_setting("FORCING#ZVAL")
        uref = self.config.get_setting("FORCING#UREF")
        uval = self.config.get_setting("FORCING#UVAL")
        zsoro_converter = self.config.get_setting("FORCING#ZSORO_CONVERTER")
        qa_converter = self.config.get_setting("FORCING#QA_CONVERTER")
        dir_sw_converter = self.config.get_setting("FORCING#DIR_SW_CONVERTER")
        sca_sw = self.config.get_setting("FORCING#SCA_SW")
        lw_converter = self.config.get_setting("FORCING#LW_CONVERTER")
        co2 = self.config.get_setting("FORCING#CO2")
        rain_converter = self.config.get_setting("FORCING#RAIN_CONVERTER")
        snow_converter = self.config.get_setting("FORCING#SNOW_CONVERTER")
        wind_converter = self.config.get_setting("FORCING#WIND_CONVERTER")
        wind_dir_converter = self.config.get_setting("FORCING#WINDDIR_CONVERTER")
        ps_converter = self.config.get_setting("FORCING#PS_CONVERTER")
        analysis = self.config.get_setting("FORCING#ANALYSIS")
        debug = self.config.get_setting("FORCING#DEBUG")
        timestep = self.config.get_setting("FORCING#TIMESTEP")
        interpolation = self.config.get_setting("FORCING#INTERPOLATION")

        kwargs.update({"input_format": input_format})
        kwargs.update({"pattern": pattern})
        kwargs.update({"zref": zref})
        kwargs.update({"zval": zval})
        kwargs.update({"uref": uref})
        kwargs.update({"uval": uval})
        kwargs.update({"zsoro_converter": zsoro_converter})
        kwargs.update({"qa_converter": qa_converter})
        kwargs.update({"dir_sw_converter": dir_sw_converter})
        kwargs.update({"sca_sw": sca_sw})
        kwargs.update({"lw_converter": lw_converter})
        kwargs.update({"co2": co2})
        kwargs.update({"rain_converter": rain_converter})
        kwargs.update({"snow_converter": snow_converter})
        kwargs.update({"wind_converter": wind_converter})
        kwargs.update({"wind_dir_converter": wind_dir_converter})
        kwargs.update({"ps_converter": ps_converter})
        kwargs.update({"debug": debug})
        kwargs.update({"timestep": timestep})
        kwargs.update({"analysis": analysis})
        kwargs.update({"interpolation": interpolation})

        if os.path.exists(output):
            logging.info("Output already exists: %s", output)
        else:
            options, var_objs, att_objs = surfex.forcing.set_forcing_config(**kwargs)
            surfex.forcing.run_time_loop(options, var_objs, att_objs)


class ModifyForcing(AbstractTask):

    """Create modify forcing task."""

    def __init__(self, config):
        """Construct modify forcing task.

        Args:
            config (dict): Actual configuration dict

        """
        AbstractTask.__init__(self, config)
        self.var_name = self.config.get_setting("TASK#VAR_NAME")
        user_config = None
        # TODO fix this test
        if "TASK" in self.config.settings:
            if "FORCING_USER_CONFIG" in self.config.settings["TASK"]:
                user_config = self.config.get_setting("TASK#FORCING_USER_CONFIG", default=None)
        self.user_config = user_config

    def execute(self):
        """Execute the forcing task.

        Raises:
            NotImplementedError: _description_
        """
        dtg = self.dtg
        fcint = self.fcint
        dtg_prev = dtg - timedelta(seconds=fcint)
        print("forcing.py_dtg_prev:", dtg, dtg_prev)
        input_dir = self.exp_file_paths.get_system_path("forcing_dir", 
                basedtg=dtg_prev, 
                default_dir="default_forcing_dir")
        output_dir = self.exp_file_paths.get_system_path("forcing_dir", 
                basedtg=dtg, 
                default_dir="default_forcing_dir")
        input_file = input_dir + "FORCING.nc"
        output_file = output_dir + "FORCING.nc"
        time_step = -1
        variables = ["LWdown", "DIR_SWdown"]
        kwargs = {}

        kwargs.update({"input_file": input_file})
        kwargs.update({"output_file": output_file})
        kwargs.update({"time_step": time_step})
        kwargs.update({"variables": variables})
        if os.path.exists(output_file) and os.path.exists(input_file):
            surfex.forcing.modify_forcing(**kwargs)
        else:
            logging.info("Output or inut is missing: %s", output_file)


