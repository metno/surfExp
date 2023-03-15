"""Default ecflow container."""
# @ENV_SUB1@
import logging


from experiment.scheduler.scheduler import EcflowTask, EcflowClient
from experiment.configuration import ConfigurationFromJsonFile
from experiment.tasks.discover_tasks import get_task

# @ENV_SUB2@


def parse_ecflow_vars():
    """Parse the ecflow variables."""
    return {
        "CONFIG": "%CONFIG%",
        "WRAPPER": "%WRAPPER%",
        "ENSMBR": "%ENSMBR%",
        "DTG": "%DTG%",
        "DTGPP": "%DTGPP%",
        "STREAM": "%STREAM%",
        "TASK_NAME": "%TASK%",
        "VAR_NAME": "%VAR_NAME%",
        "LOGLEVEL": "%LOGLEVEL%",
        "ARGS": "%ARGS%",
        "ECF_NAME": "%ECF_NAME%",
        "ECF_PASS": "%ECF_PASS%",
        "ECF_TRYNO": "%ECF_TRYNO%",
        "ECF_RID": "%ECF_RID%"
    }


'''
%nopp"
'''


def default_main(**kwargs):
    """Ecflow container default method."""
    loglevel = kwargs.get("LOGLEVEL")
    if loglevel.lower() == "debug":
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        level = logging.INFO
        if loglevel.lower() == "warning":
            level = logging.WARNING
        elif loglevel.lower() == "critical":
            level = logging.CRITICAL
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=level)

    ecf_name = kwargs.get("ECF_NAME")
    ecf_pass = kwargs.get("ECF_PASS")
    ecf_tryno = kwargs.get("ECF_TRYNO")
    ecf_rid = kwargs.get("ECF_RID")
    task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid)

    task_name = kwargs.get("TASK_NAME")
    config = kwargs.get("CONFIG")
    config = ConfigurationFromJsonFile(config)
    config.update_setting("GENERAL#STREAM", kwargs.get("STREAM"))
    config.update_setting("GENERAL#ENSMBR", kwargs.get("ENSMBR"))

    args = kwargs.get("ARGS")
    args_dict = {}
    if args != "":
        logging.debug("ARGS=%s", args)
        for arg in args.split(";"):
            parts = arg.split("=")
            logging.debug("arg=%s parts=%s len(parts)=%s", arg, parts, len(parts))
            if len(parts) == 2:
                args_dict.update({parts[0]: parts[1]})

    config.progress.update(dtg=kwargs.get("DTG"), dtgpp=kwargs.get("DTGPP"))
    task_info = {
        "WRAPPER": kwargs.get("WRAPPER"),
        "VAR_NAME": kwargs.get("VAR_NAME"),
        "ARGS": args_dict,
    }
    config.update_setting("TASK", task_info)

    # This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
    with EcflowClient(config.server, task):

        logging.info("Running task %s", task_name)
        get_task(task.ecf_task, config).run()
        logging.info("Finished task %s", task_name)


if __name__ == "__main__":
    # Get ecflow variables
    kwargs_main = parse_ecflow_vars()

    default_main(**kwargs_main)

'''    # noqa
%end"  # noqa
'''    # noqa
