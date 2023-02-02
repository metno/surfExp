"""Client interfaces for offline experiment scripts."""
import sys
from argparse import ArgumentParser
from datetime import datetime
import os
import logging
import experiment


def parse_surfex_script(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline run script")
    parser.add_argument('action', type=str, help="Action",
                        choices=["start", "prod", "continue", "testbed",
                                 "install", "climate", "co"])
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)

    parser.add_argument('-dtg', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False,
                        default=None)
    parser.add_argument('-dtgend', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False,
                        default=None)
    parser.add_argument('--suite', type=str, default="surfex", required=False,
                        help="Type of suite definition")
    parser.add_argument('--stream', type=str, default=None, required=False, help="Stream")

    # co
    parser.add_argument("--file", type=str, default=None, required=False, help="File to checkout")

    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=experiment.__version__)

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
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    logging.info("************ PySurfexExp ******************")

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

    work_dir = kwargs.get("wd")
    if work_dir is None:
        work_dir = f"{os.getcwd()}"
        logging.info("Setting working directory from current directory: %s", work_dir)

    # Find experiment
    if exp is None:
        logging.info("Setting EXP from WD: %s", work_dir)
        exp = work_dir.split("/")[-1]
        logging.info("EXP = %s", exp)

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

        progress_file = work_dir + "/progress.json"
        progress_pp_file = work_dir + "/progressPP.json"

        progress = None
        if action.lower() == "prod" or action.lower() == "continue":
            progress = experiment.ProgressFromFiles(work_dir)
            if dtgend is not None:
                progress.dtgend = datetime.strptime(dtgend, "%Y%m%d%H%M")
            if dtg is not None:
                progress.dtg = datetime.strptime(dtg, "%Y%m%d%H%M")
        else:
            if action == "start":
                if dtg is None:
                    raise Exception("No DTG was provided!")

                # Convert dtg/dtgend to datetime
                dtg = experiment.Progress.string2datetime(dtg)
                dtgend = experiment.Progress.string2datetime(dtgend)

                # Read progress from file. Returns None if no file exists or not set.
                progress = experiment.ProgressFromFiles(work_dir, stream=stream)

                dtgbeg = dtg
                if dtgend is None:
                    dtgend = progress.dtgend
                progress = experiment.Progress(dtg, dtgbeg, dtgend=dtgend)

        # Update progress
        if progress is not None:
            progress.save_as_json(work_dir, progress=True, progress_pp=True, indent=2)

        # Set experiment from files. Should be existing now after setup
        exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
        sfx_exp = experiment.ExpFromFilesDepFile(exp_dependencies_file, stream=stream)
        sfx_exp.dump_exp_configuration(f"{work_dir}/exp_configuration.json", indent=2)

        # Create and start the suite
        def_file = f"{work_dir}/{suite}.def"

        defs = experiment.get_defs(sfx_exp, suite)
        defs.save_as_defs(def_file)
        sfx_exp.server.start_suite(defs.suite_name, def_file, begin=begin)


def parse_update_config(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Update Surfex offline configuration")
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=experiment.__version__)

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
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    exp = kwargs.get("exp")

    work_dir = kwargs.get("wd")

    # Find experiment
    if work_dir is None:
        work_dir = os.getcwd()
        logging.info("Setting current working directory as WD: %s", work_dir)
    if exp is None:
        logging.info("Setting EXP from WD: %s", work_dir)
        exp = work_dir.split("/")[-1]
        logging.info("EXP = %s", exp)

    # Set experiment from files. Should be existing now after setup
    exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
    sfx_exp = experiment.ExpFromFilesDepFile(exp_dependencies_file)
    sfx_exp.dump_exp_configuration(f"{work_dir}/exp_configuration.json", indent=2)

    logging.info("Configuration was updated!")


def surfex_exp():
    """Surfex exp script entry point."""
    kwargs = parse_surfex_script(sys.argv[1:])
    surfex_script(**kwargs)


def surfex_exp_config():
    """Surfex exp config entry point."""
    kwargs = parse_update_config(sys.argv[1:])
    update_config(**kwargs)
