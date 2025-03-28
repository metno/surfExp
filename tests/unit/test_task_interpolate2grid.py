#!/usr/bin/env python3
import pytest

from deode.config_parser import ConfigParserDefaults, ParsedConfig
from deode.derived_variables import derived_variables, set_times
from deode.logs import logger  # Use deode's own configs for logger
from deode.submission import ProcessorLayout, TaskSettings
from deode.tasks.discover_task import get_task

'''
def test_task_interpolate2grid():

    logger.enable("deode")
    config = "/home/sbu/projects/surfExp/dt_offline_drammen.toml"
    config = ParsedConfig.from_file(
        config, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA
    )
    config = config.copy(update=set_times(config))
    config = config.copy(
        {"task": {
            "args": {
                "force": "true",
                "infile": "/scratch/sbu/deode/CY49DT_OFFLINE_dt_2_5_2500x2500/grib/dt+@LL@.grib1",
                "arg_defs": "an_forcing"
                }
            }
        })
    #config = config.copy(update={"platform": {"deode_home": deode_home}})

    task = "Interpolate2grid"
    task_settings = TaskSettings(config).get_task_settings(task)
    processor_layout = ProcessorLayout(task_settings)
    update = derived_variables(config, processor_layout=processor_layout)
    config = config.copy(update=update)

    logger.info("Running task {}", task)
    get_task(task, config).run()
    logger.info("Finished task {}", task)
'''

