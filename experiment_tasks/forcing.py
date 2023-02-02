"""Forcing task."""
import os
from datetime import timedelta
import json
import logging
import yaml
import surfex
from experiment_tasks import AbstractTask


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
        global_config = self.work_dir + "/config/config.yml"
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
        sca_sw = self.config.get_setting("FORCING#SCA_SW")
        co2 = self.config.get_setting("FORCING#CO2")
        rain_converter = self.config.get_setting("FORCING#RAIN_CONVERTER")
        wind_converter = self.config.get_setting("FORCING#WIND_CONVERTER")
        wind_dir_converter = self.config.get_setting("FORCING#WINDDIR_CONVERTER")
        debug = self.config.get_setting("FORCING#DEBUG")
        timestep = self.config.get_setting("FORCING#TIMESTEP")

        kwargs.update({"input_format": input_format})
        kwargs.update({"pattern": pattern})
        kwargs.update({"zref": zref})
        kwargs.update({"zval": zval})
        kwargs.update({"uref": uref})
        kwargs.update({"uval": uval})
        kwargs.update({"zsoro_converter": zsoro_converter})
        kwargs.update({"sca_sw": sca_sw})
        kwargs.update({"co2": co2})
        kwargs.update({"rain_converter": rain_converter})
        kwargs.update({"wind_converter": wind_converter})
        kwargs.update({"wind_dir_converter": wind_dir_converter})
        kwargs.update({"debug": debug})
        kwargs.update({"timestep": timestep})

        if os.path.exists(output):
            logging.info("Output already exists: %s", output)
        else:
            options, var_objs, att_objs = surfex.forcing.set_forcing_config(**kwargs)
            surfex.forcing.run_time_loop(options, var_objs, att_objs)


class MetNordicForcing(AbstractTask):
    """Create forcing task."""

    def __init__(self, config):
        """Construct forcing task.

        Args:
            config (dict): Actual configuration dict

        """

        AbstractTask.__init__(self, config)

        date_string = self.dtg.strftime("%Y%m%dT%HZ")
        yyyymmdd = self.dtg.strftime("%Y/%m/%d")

        met_nordic_production = f"/lustre/storeB/project/metproduction/products/" \
                                f"yr_short/met_analysis_1_0km_nordic_{date_string}.nc"
        met_nordic_immutable = f"/lustre/storeB/immutable/archive/projects/metproduction/" \
                               f"yr_short/{yyyymmdd}/met_analysis_1_0km_nordic_{date_string}.nc"

        logging.info("Checking MET-Nordic data")
        if os.path.exists(met_nordic_production):
            logging.info("FOUND MET-Nordic data in production=%s", met_nordic_production)
            met_nordic_pattern = met_nordic_production
        elif os.path.exists(met_nordic_immutable):
            logging.info("FOUND MET-Nordic data in immutable=%s ", met_nordic_immutable)
            met_nordic_pattern = met_nordic_immutable
        else:
            logging.info("No MET-Nordic data found in production=%s and immutable=%s ",
                         met_nordic_production, met_nordic_immutable)
            met_nordic_pattern = None

        member = None
        model_pattern1 = None
        model_pattern2 = None
        dtg = self.config.progress.dtg
        last_dtg = dtg - timedelta(hours=66)
        found_dtg = None
        while dtg > last_dtg:
            pattern = "/lustre/storeB/project/metproduction/products/meps/" \
                      "meps_det_vdiv_2_5km_@YYYY@@MM@@DD@T@HH@Z.nc"
            model_production = pattern.replace("@YYYY@", dtg.strftime("%Y"))
            model_production = model_production.replace("@MM@", dtg.strftime("%m"))
            model_production = model_production.replace("@DD@", dtg.strftime("%d"))
            model_production = model_production.replace("@HH@", dtg.strftime("%H"))
            logging.info("Checking %s", model_production)
            if os.path.exists(model_production):
                logging.info("FOUND %s", model_production)
                if found_dtg is not None:
                    if (found_dtg - dtg) >= timedelta(hours=6):
                        model_pattern2 = model_production
                if model_pattern1 is None:
                    found_dtg = dtg
                    model_pattern1 = model_production

                if model_pattern2 is not None:
                    break
            dtg = dtg - timedelta(hours=1)

        # try immutable
        if model_pattern1 is None:
            model_pattern2 = None
            dtg = self.config.progress.dtg
            last_dtg = dtg - timedelta(hours=66)
            found_dtg = None
            while dtg > last_dtg:
                pattern = "/lustre/storeB/immutable/archive/projects/metproduction/" \
                          "meps/@YYYY@/@MM@/@DD@/meps_det_2_5km_@YYYY@@MM@@DD@T@HH@Z.nc"
                model_immutable = pattern.replace("@YYYY@", dtg.strftime("%Y"))
                model_immutable = model_immutable.replace("@MM@", dtg.strftime("%m"))
                model_immutable = model_immutable.replace("@DD@", dtg.strftime("%d"))
                model_immutable = model_immutable.replace("@HH@", dtg.strftime("%H"))
                logging.info("Checking %s", model_immutable)
                if os.path.exists(model_immutable):
                    logging.info("FOUND %s", model_immutable)
                    if found_dtg is not None:
                        if (found_dtg - dtg) >= timedelta(hours=6):
                            model_pattern2 = model_production
                    if model_pattern1 is None:
                        found_dtg = dtg
                        model_pattern1 = model_production

                if model_pattern2 is not None:
                    break
                dtg = dtg - timedelta(hours=1)

        if model_pattern1 is None and model_pattern2 is None:
            raise Exception("No suitable model data found!")

        self.config.update_setting("FORCING#PATTERN", model_pattern1)
        self.config.update_setting("FORCING#ZREF", "screen")
        self.config.update_setting("FORCING#UREF", "screen")
        self.config.update_setting("FORCING#ZVAL", "2")
        self.config.update_setting("FORCING#UVAL", "10")

        self.config.update_setting("FORCING#TA", "none")
        self.config.update_setting("FORCING#QA", "rh2q")
        self.config.update_setting("FORCING#ZSORO_CONVERTER", "phi2m")
        self.config.update_setting("FORCING#SCA_SW", "constant")
        self.config.update_setting("FORCING#CO2", "constant")
        self.config.update_setting("FORCING#RAIN_CONVERTER", "calcrain")
        self.config.update_setting("FORCING#SNOW_CONVERTER", "calcsnow")
        self.config.update_setting("FORCING#WIND_CONVERTER", "windspeed")
        self.config.update_setting("FORCING#WINDDIR_CONVERTER", "winddir")

        if met_nordic_pattern is None:
            logging.info("No analysis found. Use forecast only!")
            self.config.update_setting("FORCING#RAIN_CONVERTER", "totalprec")
            self.config.update_setting("FORCING#WIND_CONVERTER", "none")
            ta_none_converter = {
                "name": "air_temperature_2m",
                "level": 2,
                "units": "K",
                "filepattern": model_pattern1
            }
            rh2q = {
                "rh": {
                    "name": "relative_humidity_2m",
                    "level": 2,
                    "units": "%",
                    "filepattern": model_pattern1
                },
                "t": {
                    "name": "air_temperature_2m",
                    "level": 2,
                    "units": "K",
                    "filepattern": model_pattern1,
                },
                "p": {
                    "name": "surface_air_pressure",
                    "filepattern": model_pattern1,
                    "units": "Pa",
                    "member": member
                }
            }
        else:
            ta_none_converter = {
                    "name": "air_temperature_2m",
                    "level": 2,
                    "units": "K",
                    "filepattern": met_nordic_pattern
            }
            rh2q = {
                "rh": {
                    "name": "relative_humidity_2m",
                    "level": 2,
                    "units": "%",
                    "filepattern": met_nordic_pattern,
                    "interpolator": "bilinear"
                },
                "t": {
                    "name": "air_temperature_2m",
                    "level": 2,
                    "units": "K",
                    "filepattern": met_nordic_pattern
                },
                "p": {
                    "name": "surface_air_pressure",
                    "filepattern": model_pattern1,
                    "units": "Pa",
                    "member": member
                }
            }

        user_config = {
            "QA": {
                "screen": {
                    "netcdf": {
                        "converter": {
                            "none": {
                               "name": "specific_humidity_2m"
                            }
                        }
                    }
                }
            },
            "TA": {
                "screen": {
                    "netcdf": {
                        "converter": {
                            "none": {
                               "name": "air_temperature_2m"
                            }
                        }
                    }
                }
            },
            "PS": {
                "netcdf": {
                    "converter": {
                        "none": {
                            "name": "surface_air_pressure",
                            "filepattern": model_pattern1,
                            "units": "Pa",
                            "member": member
                        }
                    }
                }
            },
            "DIR_SW": {
                "netcdf": {
                    "converter": {
                        "none": {
                            "filepattern": model_pattern2,
                            "name": "integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time",
                            "accumulated": True,
                            "instant": 3600.0,
                            "member": member
                        }
                    }
                }
            },
            "LW": {
                "netcdf": {
                    "converter": {
                        "none": {
                            "filepattern": model_pattern2,
                            "name": "integral_of_surface_downwelling_longwave_flux_in_air_wrt_time",
                            "accumulated": True,
                            "instant": 3600.0,
                            "member": member
                        }
                    }
                }
            },
            "RAIN": {
                "netcdf": {
                    "converter": {
                        "calcrain": {
                            "totalprec": {
                                "filepattern": met_nordic_pattern,
                                "name": "precipitation_amount",
                                "accumulated": False,
                                "instant": 3600.0
                            }
                        },
                        "totalprec": {
                            "totalprec": {
                                "filepattern": model_pattern2,
                                "name": "precipitation_amount_acc",
                                "accumulated": True,
                                "instant": 3600.,
                                "member": member
                            },
                            "snow": {
                                "filepattern": model_pattern2,
                                "name": "snowfall_amount_acc",
                                "accumulated": True,
                                "instant": 3600.,
                                "member": member
                            }
                        }
                    }
                }
            },
            "SNOW": {
                "netcdf": {
                    "converter": {
                        "none": {
                            "filepattern": model_pattern2,
                            "name": "snowfall_amount_acc",
                            "accumulated": True,
                            "instant": 3600.,
                            "member": member
                        },
                        "calcsnow": {
                            "totalprec": {
                                "name": "precipitation_amount",
                                "instant": 3600.0
                            }
                        }
                    }
                }
            }
        }
        user_config["QA"]["screen"]["netcdf"]["converter"]["rh2q"] = rh2q
        user_config["TA"]["screen"]["netcdf"]["converter"]["none"] = ta_none_converter
        user_config_file = f"{self.wrk}/user_config.json"
        self.config.update_setting("TASK#FORCING_USER_CONFIG", user_config_file)
        with open(user_config_file, mode="w", encoding="utf-8") as uconfig:
            yaml.dump(user_config, uconfig, indent=2)
        self.user_config = user_config_file

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
        global_config = self.work_dir + "/config/config.yml"
        with open(global_config, mode="r", encoding="utf-8") as file_handler:
            global_config = yaml.safe_load(file_handler)
        kwargs.update({"config": global_config})

        kwargs.update({"dtg_start": dtg.strftime("%Y%m%d%H")})
        kwargs.update({"dtg_stop": dtg.strftime("%Y%m%d%H")})

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
        sca_sw = self.config.get_setting("FORCING#SCA_SW")
        co2 = self.config.get_setting("FORCING#CO2")
        rain_converter = self.config.get_setting("FORCING#RAIN_CONVERTER")
        wind_converter = self.config.get_setting("FORCING#WIND_CONVERTER")
        wind_dir_converter = self.config.get_setting("FORCING#WINDDIR_CONVERTER")
        debug = self.config.get_setting("FORCING#DEBUG")

        kwargs.update({"input_format": input_format})
        kwargs.update({"pattern": pattern})
        kwargs.update({"zref": zref})
        kwargs.update({"zval": zval})
        kwargs.update({"uref": uref})
        kwargs.update({"uval": uval})
        kwargs.update({"zsoro_converter": zsoro_converter})
        kwargs.update({"sca_sw": sca_sw})
        kwargs.update({"co2": co2})
        kwargs.update({"rain_converter": rain_converter})
        kwargs.update({"wind_converter": wind_converter})
        kwargs.update({"wind_dir_converter": wind_dir_converter})

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
            logging.info("Output or inut is missing: %s", output)


