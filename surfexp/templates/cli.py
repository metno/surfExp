"""Entry point to execute a task in a template"""
import json
import sys

from .ecflow.default import default_main
from .stand_alone import stand_alone_main


def execute_task(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args_file = argv[0]
    with open(args_file, mode="r", encoding="utf8") as fhandler:
        kwargs = json.load(fhandler)
    template = kwargs.get("template")
    if template is None:
        template = "ecflow"
    if template == "ecflow":
        default_main(**kwargs)
    elif template == "stand_alone":
        task_name = kwargs["STAND_ALONE_TASK_NAME"]
        config = kwargs["STAND_ALONE_TASK_CONFIG"]
        deode_home = kwargs["STAND_ALONE_DEODE_HOME"]
        stand_alone_main(task_name, config, deode_home)
    else:
        raise NotImplementedError
