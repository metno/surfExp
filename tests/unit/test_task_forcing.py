#!/usr/bin/env python3
import pytest
#import sys
#sys.path.insert(0, "/home/sbu/projects/multi_path_search/")
from deode.config_parser import ConfigParserDefaults, ParsedConfig
from deode.derived_variables import derived_variables, set_times
from deode.logs import logger  # Use deode's own configs for logger
from deode.submission import ProcessorLayout, TaskSettings
from deode.tasks.discover_task import get_task


def test_task_forcing():

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
                "forcing_user_config": "/home/sbu/projects/surfExp/surfexp/data/config/forcing/forcing_dt_config.yml",
                "arg_defs": "an_forcing"
                }
            }
        })
    #config = config.copy(update={"platform": {"deode_home": deode_home}})

    task = "Forcing"
    task_settings = TaskSettings(config).get_task_settings(task)
    processor_layout = ProcessorLayout(task_settings)
    update = derived_variables(config, processor_layout=processor_layout)
    config = config.copy(update=update)

    logger.info("Running task {}", task)
    get_task(task, config).run()
    logger.info("Finished task {}", task)
