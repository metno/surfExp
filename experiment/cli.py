"""Client interfaces for offline experiment scripts."""
import sys
from argparse import ArgumentParser
from datetime import datetime
import os


from . import PACKAGE_NAME, __version__

from .experiment import ExpFromFilesDepFile
from .scheduler.submission import NoSchedulerSubmission, TaskSettings, TroikaSettings
from .scheduler.scheduler import EcflowServerFromConfig
from .suites import get_defs
from .progress import ProgressFromFiles, Progress
from .logs import get_logger
from .config_parser import ParsedConfig


def parse_surfex_script(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline run script")
    parser.add_argument('action', type=str, help="Action",
                        choices=["start", "prod", "continue", "testbed",
                                 "install", "climate", "co"])
    parser.add_argument('-exp_name', dest="exp", help="Experiment name",
                        type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory",
                        type=str, default=None)

    parser.add_argument('-dtg', help="DateTimeGroup (YYYYMMDDHH)",
                        type=str, required=False,
                        default=None)
    parser.add_argument('-dtgend', help="DateTimeGroup (YYYYMMDDHH)",
                        type=str, required=False,
                        default=None)
    parser.add_argument('--suite', type=str, default="surfex", required=False,
                        help="Type of suite definition")
    parser.add_argument('--stream', type=str, default=None, required=False,
                        help="Stream")

    # co
    parser.add_argument("--file", type=str, default=None, required=False,
                        help="File to checkout")

    parser.add_argument('--debug', dest="debug", action="store_true",
                        help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def surfex_script(**kwargs):
    """Modify or start an experiment suite.

    Raises:
        NotImplementedError: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
    """
    debug = kwargs.get("debug")
    if debug is None:
        debug = False

    if debug:
        loglevel = "DEBUG"
    else:
        loglevel = "INFO"

    logger = get_logger(PACKAGE_NAME, loglevel)
    logger.info("************ PySurfexExp ******************")

    action = kwargs["action"]
    exp = kwargs.get("exp")

    stream = kwargs.get("stream")

    # Others
    dtg = kwargs["dtg"]
    dtgend = kwargs["dtgend"]
    suite = kwargs["suite"]

    begin = kwargs.get("begin")
    if begin is None:
        begin = True

    logger.info("debug %s", debug)
    work_dir = kwargs.get("wd")
    if work_dir is None:
        work_dir = f"{os.getcwd()}"
        logger.info("Setting working directory from current directory: %s", work_dir)

    # Find experiment
    if exp is None:
        logger.info("Setting EXP from WD: %s", work_dir)
        exp = work_dir.split("/")[-1]
        logger.info("EXP = %s", exp)

    if "action" == "mon":
        # TODO

        raise NotImplementedError
    else:
        # Some kind of start
        if action == "start" and dtg is None:
            raise Exception("You must provide -dtg to start a simulation")
        elif action == "climate":
            if dtg is None:
                dtg = "2008061600"
            if suite is not None and suite != "climate":
                raise Exception("Action was climate but you also specified a suite not being " +
                                + f"climate: {suite}")
            suite = "climate"
        elif action == "testbed":
            if dtg is None:
                dtg = "200806160000"
            if suite is not None and suite != "testbed":
                raise Exception("Action was climate but you also specified a suite not being " +
                                + f"testbed: {suite}")
            suite = "testbed"
        elif action == "install":
            if dtg is None:
                dtg = "200806160000"
            raise NotImplementedError

        # progress_file = work_dir + "/progress.json"
        # progress_pp_file = work_dir + "/progressPP.json"

        progress = None
        if action.lower() == "prod" or action.lower() == "continue":
            progress = ProgressFromFiles(work_dir)
            if dtgend is not None:
                progress.dtgend = datetime.strptime(dtgend, "%Y%m%d%H%M")
            if dtg is not None:
                progress.dtg = datetime.strptime(dtg, "%Y%m%d%H%M")
        else:
            if action == "start":
                if dtg is None:
                    raise Exception("No DTG was provided!")

                # Convert dtg/dtgend to datetime
                dtg = Progress.string2datetime(dtg)
                dtgend = Progress.string2datetime(dtgend)

                # Read progress from file. Returns None if no file exists or not set.
                try:
                    progress = ProgressFromFiles(work_dir, dtg=dtg, dtgbeg=dtg, dtgend=dtgend,
                                                 dtgpp=dtg, stream=stream)
                except FileNotFoundError:
                    progress = Progress(dtg, dtg, dtgend=dtgend, dtgpp=dtg, stream=stream)


        # Update progress
        if progress is not None:
            progress.save_as_json(work_dir, progress=True, progress_pp=True, indent=2)

        # Set experiment from files. Should be existing now after setup
        exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
        sfx_exp = ExpFromFilesDepFile(exp_dependencies_file, stream=stream)
        sfx_exp.dump_json(f"{work_dir}/exp_configuration.json", indent=2)

        # Create and start the suite
        def_file = f"{work_dir}/{suite}.def"

        defs = get_defs(sfx_exp.config, suite)
        defs.save_as_defs(def_file)
        server = EcflowServerFromConfig(sfx_exp.config)
        server.start_suite(defs.suite_name, def_file, begin=begin)


def parse_update_config(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Update Surfex offline configuration")
    parser.add_argument('-exp_name', dest="exp", help="Experiment name",
                        type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory",
                        type=str, default=None)
    parser.add_argument('--debug', dest="debug", action="store_true",
                        help="Debug information")
    parser.add_argument('--version', action='version',
                        version=__version__)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def update_config(**kwargs):
    """Update the experiment json file configurations."""
    debug = kwargs.get("debug")
    if debug is None:
        debug = False
    if debug:
        loglevel = "DEBUG"
    else:
        loglevel = "INFO"

    logger = get_logger(PACKAGE_NAME, loglevel)
    exp = kwargs.get("exp")

    work_dir = kwargs.get("wd")

    # Find experiment
    if work_dir is None:
        work_dir = os.getcwd()
        logger.info("Setting current working directory as WD: %s", work_dir)
    if exp is None:
        logger.info("Setting EXP from WD: %s", work_dir)
        exp = work_dir.split("/")[-1]
        logger.info("EXP = %s", exp)

    # Set experiment from files. Should be existing now after setup
    exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
    sfx_exp = ExpFromFilesDepFile(exp_dependencies_file)
    sfx_exp.dump_json(f"{work_dir}/exp_configuration.json", indent=2)

    logger.info("Configuration was updated!")


def surfex_exp(argv=None):
    """Surfex exp script entry point."""
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_surfex_script(argv)
    surfex_script(**kwargs)


def surfex_exp_config(argv=None):
    """Surfex exp config entry point."""
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_update_config(argv)
    update_config(**kwargs)


def parse_submit_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("ECF_submit task to ecflow")
    parser.add_argument('-config', dest="config_file", type=str,
                        help="Configuration file")
    parser.add_argument('-task', type=str, help="Task name")
    parser.add_argument('-task_job', type=str, help="Task job file",
                        required=False, default=None)
    parser.add_argument('-output', type=str, help="Output file",
                        required=False, default=None)
    parser.add_argument('-template', dest="template_job", type=str,
                        help="Template", required=False, default=None)
    parser.add_argument('-troika', type=str, help="Troika", required=False,
                        default=None)
    parser.add_argument('--debug', dest="debug", action="store_true",
                        help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit()

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def submit_cmd_exp(**kwargs):
    """Submit task."""
    debug = kwargs.get("debug")
    if debug is None:
        debug = False
    if debug:
        loglevel = "DEBUG"
    else:
        loglevel = "INFO"

    logger = get_logger(PACKAGE_NAME, loglevel)
    logger.info("************ ECF_submit_exp ******************")

    logger.debug("kwargs %s", str(kwargs))
    config_file = kwargs.get("config_file")
    cwd = os.getcwd()
    if config_file is None:
        config_file = f"{cwd}/exp_configuration.json"
        logger.info("Using config file=%s", config_file)
        if os.path.exists("exp_configuration.json"):
            logger.info("Using config file=%s", config_file)
        else:
            raise FileNotFoundError("Could not find config file " + config_file)
    config = ParsedConfig.from_file(config_file)
    task = kwargs.get("task")
    
    template_job = kwargs.get("template_job")
    if template_job is None:
        scripts = config.get_value("general.pysurfex_experiment")
        if scripts is None:
            raise Exception("Could not find general.pysurfex_experiment")
        else:
            template_job = f"{scripts}/experiment/templates/stand_alone.py"
    task_job = kwargs.get("task_job")
    if task_job is None:
        task_job = f"{cwd}/{task}.job"
    output = kwargs.get("output")
    if output is None:
        output = f"{cwd}/{task}.log"
    logger.debug("Task: %s", task)
    logger.debug("config: %s", config_file)
    logger.debug("template_job: %s", template_job)
    logger.debug("task_job: %s", task_job)
    logger.debug("output: %s", output)
    submission_defs = TaskSettings(config)
    sub = NoSchedulerSubmission(submission_defs)
    sub.submit(
        kwargs.get("task"),
        config,
        template_job,
        task_job,
        output
    )


def run_submit_cmd_exp(argv=None):
    """Run submit."""
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_submit_cmd_exp(argv)
    submit_cmd_exp(**kwargs)
