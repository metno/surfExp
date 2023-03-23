"""Default ecflow container."""
# @ENV_SUB1@


from experiment.config_parser import ParsedConfig
from experiment.logs import get_logger_from_config
from experiment.tasks.discover_tasks import get_task

# @ENV_SUB2@


def stand_alone_main(task, config_file):
    """Execute default main.

    Args:
        task (str): Task name
        config_file (str): Config file
    """
    config = ParsedConfig.from_file(config_file)
    logger = get_logger_from_config(config)

    logger.info("Running task %s", task)

    get_task(task, config).run()
    logger.info("Finished task %s", task)


if __name__ == "__main__":
    TASK_NAME = "@STAND_ALONE_TASK_NAME@"
    CONFIG = "@STAND_ALONE_TASK_CONFIG@"

    stand_alone_main(TASK_NAME, CONFIG)
