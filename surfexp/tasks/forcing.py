"""Forcing task."""
import json
import os

import pysurfex
import yaml
from deode.logs import logger
from pysurfex.forcing import modify_forcing, run_time_loop, set_forcing_config

from surfexp.tasks.tasks import PySurfexBaseTask


class Forcing(PySurfexBaseTask):
    """Create forcing task."""

    def __init__(self, config):
        """Construct forcing task.

        Args:
            config (dict): Actual configuration dict

        """
        PySurfexBaseTask.__init__(self, config, "Forcing")
        try:
            self.var_name = self.config["task.var_name"]
        except KeyError:
            self.var_name = None
        try:
            user_config = self.config["task.forcing_user_config"]
        except KeyError:
            user_config = None
        self.user_config = user_config

    def execute(self):
        """Execute the forcing task.

        Raises:
            NotImplementedError: _description_

        """
        kwargs = {}
        if self.user_config is not None:
            with open(self.user_config, mode="r", encoding="utf-8") as fh:
                user_config = yaml.safe_load(fh)
            kwargs.update({"user_config": user_config})

        domain_json = self.geo.json
        domain_json.update({"nam_pgd_grid": {"cgrid": "CONF PROJ"}})
        with open(self.wdir + "/domain.json", mode="w", encoding="utf-8") as file_handler:
            json.dump(domain_json, file_handler, indent=2)
        kwargs.update({"domain": self.wdir + "/domain.json"})
        # global_config = self.platform.get_system_value("config_yml")
        try:
            global_config = self.config["pysurfex.forcing_variable_config_yml_file"]
        except KeyError:
            global_config = None
        if global_config is None or global_config == "":
            global_config = (
                f"{os.path.dirname(pysurfex.__path__[0])}/pysurfex/cfg/config.yml"
            )
        with open(global_config, mode="r", encoding="utf-8") as file_handler:
            global_config = yaml.safe_load(file_handler)
        kwargs.update({"config": global_config})

        kwargs.update({"dtg_start": self.dtg.strftime("%Y%m%d%H")})
        kwargs.update({"dtg_stop": (self.dtg + self.fcint).strftime("%Y%m%d%H")})

        forcing_dir = self.platform.get_system_value("forcing_dir")
        forcing_dir = self.platform.substitute(forcing_dir, basetime=self.dtg)
        os.makedirs(forcing_dir, exist_ok=True)

        output_format = self.config["SURFEX.IO.CFORCING_FILETYPE"].lower()
        if output_format == "netcdf":
            output = forcing_dir + "/FORCING.nc"
        else:
            raise NotImplementedError(output_format)

        kwargs.update({"of": output})
        kwargs.update({"output_format": output_format})

        pattern = self.config["forcing.pattern"]
        input_format = self.config["forcing.input_format"]
        kwargs.update({"geo_input_file": self.config["forcing.input_geo_file"]})
        zref = self.config["forcing.zref"]
        zval = self.config["forcing.zval"]
        uref = self.config["forcing.uref"]
        uval = self.config["forcing.uval"]
        zsoro_converter = self.config["forcing.zsoro_converter"]
        qa_converter = self.config["forcing.qa_converter"]
        dir_sw_converter = self.config["forcing.dir_sw_converter"]
        sca_sw = self.config["forcing.sca_sw"]
        lw_converter = self.config["forcing.lw_converter"]
        co2 = self.config["forcing.co2"]
        rain_converter = self.config["forcing.rain_converter"]
        snow_converter = self.config["forcing.snow_converter"]
        wind_converter = self.config["forcing.wind_converter"]
        wind_dir_converter = self.config["forcing.winddir_converter"]
        ps_converter = self.config["forcing.ps_converter"]
        analysis = self.config["forcing.analysis"]
        debug = self.config["forcing.debug"]
        timestep = self.config["forcing.timestep"]
        interpolation = self.config["forcing.interpolation"]

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
            logger.info("Output already exists: {}", output)
        else:
            options, var_objs, att_objs = set_forcing_config(**kwargs)
            run_time_loop(options, var_objs, att_objs)


class ModifyForcing(PySurfexBaseTask):
    """Create modify forcing task."""

    def __init__(self, config):
        """Construct modify forcing task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "ModifyForcing")
        self.var_name = self.config["task.var_name"]
        try:
            user_config = self.config["task.forcing_user_config"]
        except AttributeError:
            user_config = None
        self.user_config = user_config

    def execute(self):
        """Execute the forcing task."""
        dtg = self.dtg
        dtg_prev = dtg - self.fcint
        logger.debug("modify forcing dtg={} dtg_prev={}", dtg, dtg_prev)
        forcing_dir = self.platform.get_system_value("forcing_dir")
        input_dir = self.platform.substitute(forcing_dir, basetime=dtg_prev)
        output_dir = self.platform.substitute(forcing_dir, basetime=dtg)
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
            modify_forcing(**kwargs)
        else:
            logger.info("Output or input is missing: {}", output_file)
