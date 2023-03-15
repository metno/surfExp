"""Default ecflow container."""
# @ENV_SUB1@
import logging


from experiment.configuration import ConfigurationFromJsonFile
from experiment.tasks.discover_tasks import get_task

# @ENV_SUB2@


def stand_alone_main(task, config_file, loglevel):
    """Execute default main.

    Args:
        task (str): Task name
        config (str.): Config file
        loglevel (str): Loglevel
    """

    if loglevel.lower() == "debug":
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        level = logging.INFO
        if loglevel.lower() == "warning":
            level = logging.WARNING
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=level)

    logging.info("Running task %s", task)
    config = ConfigurationFromJsonFile(config_file)

    get_task(task, config).run()
    logging.info("Finished task %s", task)


if __name__ == "__main__":
    TASK_NAME = "@STAND_ALONE_TASK_NAME@"
    LOGLEVEL = "@STAND_ALONE_TASK_LOGLEVEL@"
    CONFIG = "@STAND_ALONE_TASK_CONFIG@"

    stand_alone_main(TASK_NAME, CONFIG, LOGLEVEL)
