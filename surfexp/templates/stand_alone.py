"""NoSchedulerTemplate."""

import os

from tactus.config_parser import ConfigParserDefaults, ParsedConfig
from tactus.derived_variables import derived_variables, set_times
from tactus.logs import logger  # Use tactus's own configs for logger
from tactus.submission import ProcessorLayout, TaskSettings
from tactus.tasks.discover_task import get_task

logger.enable("tactus")


def stand_alone_main(task, config, deode_home):
    """Execute default main.

    Args:
        task (str): Task name
        config (str): Config file
        deode_home(str): Tactus home path
    """
    config = ParsedConfig.from_file(
        config, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA
    )
    config = config.copy(update=set_times(config))
    config = config.copy(update={"platform": {"deode_home": deode_home}})

    task_settings = TaskSettings(config).get_task_settings(task)
    processor_layout = ProcessorLayout(task_settings)
    update = derived_variables(config, processor_layout=processor_layout)
    config = config.copy(update=update)

    logger.info("Running task {}", task)
    get_task(task, config).run()
    logger.info("Finished task {}", task)


if __name__ == "__main__":
    TASK_NAME = os.environ["STAND_ALONE_TASK_NAME"]
    CONFIG = os.environ["STAND_ALONE_TASK_CONFIG"]
    DEODE_HOME = os.environ["STAND_ALONE_DEODE_HOME"]
    stand_alone_main(TASK_NAME, CONFIG, DEODE_HOME)
