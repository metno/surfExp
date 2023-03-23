"""PySurfexExpSetup functionality."""
import os
import sys
from argparse import ArgumentParser

try:
    import surfex
except:  # noqa
    surfex = None


from experiment import PACKAGE_NAME, __version__
from experiment.experiment import ExpFromFiles, ExpFromFilesDep

from ..logs import get_logger


def surfex_exp_setup(argv=None):
    """Set up PySurfex experiment.

    Args:
        argv (list, optional): Arguments. Defaults to None.

    """
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_surfex_script_setup(argv)
    surfex_script_setup(**kwargs)


def parse_surfex_script_setup(argv):
    """Parse the command line input arguments.

    Args:
        argv (list): Arguments

    Returns:
        dict: kwargs
    """
    parser = ArgumentParser("Surfex offline setup script")
    parser.add_argument(
        "-exp_name", dest="exp", help="Experiment name", type=str, default=None
    )
    parser.add_argument(
        "--wd", help="Experiment working directory", type=str, default=None
    )

    # Setup variables
    parser.add_argument(
        "-rev",
        dest="rev",
        help="Surfex experiement source revison",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "-experiment",
        dest="pysurfex_experiment",
        help="Pysurfex-experiment library",
        type=str,
        required=True,
        default=None,
    )
    parser.add_argument(
        "-offline",
        dest="offline_source",
        help="Offline source code",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "-namelist",
        dest="namelist_dir",
        help="Namelist directory",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "-host",
        dest="host",
        help="Host label for setup files",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument("--config", help="Config", type=str, required=False, default=None)
    parser.add_argument(
        "--config_file", help="Config file", type=str, required=False, default=None
    )
    parser.add_argument(
        "-o",
        dest="output_file",
        help="Output file file",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--debug", dest="debug", action="store_true", help="Debug information"
    )
    parser.add_argument("--version", action="version", version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def surfex_script_setup(**kwargs):
    """Do experiment setup.

    Args:
        kwargs (dict): Arguments

    Raises:
        RuntimeError: Setup failed
    """
    debug = kwargs.get("debug")
    if debug is None:
        debug = False
    if debug:
        loglevel = "DEBUG"
    else:
        loglevel = "INFO"

    print(loglevel)
    logger = get_logger(PACKAGE_NAME, loglevel=loglevel)
    logger.info("************ PySurfexExpSetup ******************")

    # Setup
    exp_name = kwargs.get("exp")
    wdir = kwargs.get("wd")
    pysurfex = f"{os.path.dirname(surfex.__file__)}/../"
    pysurfex_experiment = kwargs.get("pysurfex_experiment")
    offline_source = kwargs.get("offline_source")
    namelist_dir = kwargs.get("namelist_dir")
    host = kwargs.get("host")
    if host is None:
        raise RuntimeError("You must set a host")

    config = kwargs.get("config")
    config_file = kwargs.get("config_file")
    output_file = kwargs.get("output_file")
    write_config_files = True
    if output_file is not None:
        write_config_files = False

    # Find experiment
    if wdir is None:
        if output_file is None:
            wdir = os.getcwd()
            logger.info("Setting current working directory as WD: %s", wdir)
        else:
            wdir = None
    if exp_name is None:
        logger.info("Setting EXP from WD: %s", wdir)
        exp_name = wdir.split("/")[-1]
        logger.info("EXP = %s", exp_name)

    if offline_source is None:
        logger.warning("No offline soure code set. Assume existing binaries")

    exp_dependencies = ExpFromFiles.setup_files(
        wdir,
        exp_name,
        host,
        pysurfex,
        pysurfex_experiment,
        offline_source=offline_source,
        namelist_dir=namelist_dir,
        loglevel=loglevel,
    )

    # Merge and update config
    merged_config = ExpFromFiles.write_exp_config(
        exp_dependencies,
        configuration=config,
        configuration_file=config_file,
        write_config_files=write_config_files,
        loglevel=loglevel,
    )

    if output_file is None:
        # Redo exp_dependencies with local changes
        talk_level = "FATAL"
        if loglevel != "INFO":
            talk_level = loglevel
        exp_dependencies = ExpFromFiles.setup_files(
            wdir,
            exp_name,
            host,
            pysurfex,
            pysurfex_experiment,
            offline_source=offline_source,
            namelist_dir=namelist_dir,
            loglevel=talk_level,
        )

        # Save experiment dependencies
        exp_dependencies_file = wdir + "/exp_dependencies.json"
        logger.info("Store exp dependencies in %s", exp_dependencies_file)
        ExpFromFiles.dump_exp_dependencies(exp_dependencies, exp_dependencies_file)
    else:
        # Create en experiment object and dump the configuration
        merged_config = ExpFromFiles.merge_dict_from_config_dicts(
            merged_config, loglevel=loglevel
        )
        sfx_exp = ExpFromFilesDep(
            exp_dependencies, config_settings=merged_config, loglevel=loglevel
        )
        sfx_exp.dump_json(output_file, indent=2)
