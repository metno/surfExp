"""Forcing task."""
import json
import os

import yaml
from datetime import timedelta
from deode.logs import logger
from pysurfex.forcing import modify_forcing, run_time_loop, set_forcing_config
from pysurfex.verification import converter2ds, concat_datasets

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
            self.var_name = self.config["task.args.var_name"]
        except KeyError:
            self.var_name = None
        try:
            user_config = self.config["task.args.forcing_user_config"]
        except KeyError:
            user_config = None
        if user_config is not None:
            logger.info("Using user config: {}", user_config)
        self.user_config = user_config
        try:
            self.force = self.config["task.args.force"]
            self.force = bool(self.force)
        except KeyError:
            self.force = False

        try:
            self.arg_defs = f"args.{self.config['task.args.arg_defs']}"
        except KeyError:
            self.arg_defs = "args"

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
        climdir = self.platform.get_system_value("climdir")
        domain_json.update({"nam_pgd_grid": {"cgrid": "CONF PROJ"}})
        domain_file = f"{climdir}/domain.json"
        if not os.path.exists(domain_file):
            with open(domain_file, mode="w", encoding="utf-8") as file_handler:
                json.dump(domain_json, file_handler, indent=2)
        kwargs.update({"domain": domain_file})
        try:
            global_config = self.config["pysurfex.forcing_variable_config_yml_file"]
        except KeyError:
            global_config = None
        
        #TODO Global config should be handled in pysurfex

        #if global_config is None or global_config == "":
        #    global_config = (
        #        f"{os.path.dirname(pysurfex.__path__[0])}/pysurfex/cfg/config.yml"
        #    )
        #with open(global_config, mode="r", encoding="utf-8") as file_handler:
        #    global_config = yaml.safe_load(file_handler)
        # Add surfExp related macros
        global_config["macros"] = {
            "casedir": self.platform.get_system_value("casedir")
        }
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

        try:
            args = self.config[f"forcing.{self.arg_defs}"]
        except KeyError:
            logger.warning("No forcing arguments found for {}. Using default values.", self.arg_defs)
            args = {}

        for key, value in args.items():
            value = self.platform.substitute(value)
            if key in kwargs:
                logger.info("Override setting {} with value {}. New value: {}", key, kwargs[key], value)
            kwargs.update({key: value})

        if os.path.exists(output) and not self.force:
            logger.info("Output already exists: {}", output)
        else:
            if os.path.exists(output):
                logger.info("Overwrite output: {}", output)
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


class Interpolate2grid(PySurfexBaseTask):
    """Create modify forcing task."""

    def __init__(self, config):
        """Construct modify forcing task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        PySurfexBaseTask.__init__(self, config, "Interpolate2grid")
        try:
            step = self.config["task.args.step"]
        except KeyError:
            step = None
        if step is None:
            self.steps = range(0, 25)
        else:
            self.steps = [int(step)]

    def execute(self):
        vars=[
            "surface_geopotential",
            "air_temperature_2m",
            "dew_point_temperature_2m",
            "surface_air_pressure",
            "x_wind_10m",
            "y_wind_10m",
            "precipitation_amount_acc",
            "snowfall_amount_acc",
            "integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time",
            "integral_of_surface_downwelling_longwave_flux_in_air_wrt_time"
        ]

        domain_file = "domain.json"
        domain_json = self.geo.json
        domain_json.update({"nam_pgd_grid": {"cgrid": "CONF PROJ"}})
        with open(domain_file, mode="w", encoding="utf-8") as file_handler:
            json.dump(domain_json, file_handler, indent=2)
        ncdir = f"{self.platform.get_system_value('casedir')}/grib"
        gribdir = ncdir

        mapping = {
            "surface_geopotential": {
                "indicatorOfParameter": 129
            },
            "air_temperature_2m": {
                "indicatorOfParameter": 167
            },
            "dew_point_temperature_2m": {
                "indicatorOfParameter": 168
            },
            "surface_air_pressure": {
                "indicatorOfParameter": 134
            },
            "x_wind_10m": {
                "indicatorOfParameter": 165
            },
            "y_wind_10m": {
                "indicatorOfParameter": 166
            },
            "precipitation_amount_acc": {
                "indicatorOfParameter": 228,
                "timeRangeIndicator": 4
            },
            "snowfall_amount_acc": {
                "indicatorOfParameter": 144,
                "timeRangeIndicator": 4
            },
            "integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time": {
                "indicatorOfParameter": 169,
                "timeRangeIndicator": 4
            },
            "integral_of_surface_downwelling_longwave_flux_in_air_wrt_time": {
                "indicatorOfParameter": 175,
                "timeRangeIndicator": 4
            }
        }

        for leadtime in self.steps:
            validtime = self.dtg + timedelta(hours=leadtime)
            validtime = validtime.strftime("%Y%m%d%H")
            ofiles = []
            for var in vars:
                try:
                    timeRangeIndicator = mapping[var]["timeRangeIndicator"]
                except KeyError:
                    timeRangeIndicator = 0
                indicatorOfParameter = mapping[var]["indicatorOfParameter"]
                output = f"{ncdir}/{var}_{self.dtg.strftime('%Y%m%d%H')}+{leadtime:02d}.nc"
                input_file = f"{gribdir}/dt_{self.dtg.strftime('%Y%m%d%H')}+{leadtime:02d}.grib1"
                ofiles.append(output)
                argv = [
                    "-g", domain_file,
                    "-o", output,
                    "converter",
                    "-i", input_file,
                    "-it", "grib1",
                    "--indicatorOfParameter", f"{indicatorOfParameter}",
                    "--levelType", "1",
                    "--level", "0",
                    "--timeRangeIndicator", f"{timeRangeIndicator}",
                    "-v", var,
                    "-t", validtime
                ]
                converter2ds(argv=argv)

            argv = [
                "-o", f"{ncdir}/dt_{self.dtg.strftime('%Y%m%d%H')}+{leadtime:02d}.nc",
            ]
            argv = argv + ofiles
            concat_datasets(argv=argv)
