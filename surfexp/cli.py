"""Command line interface."""
import argparse
import os
import shutil
import sys

import deode
from deode.__main__ import main
from deode.logs import logger

import surfexp


def pysfxexp(argv=None):
    """Set up surfExp configuration.

    Args:
        argv (list, optional): Command arguments. Defaults to None.
    """
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", dest="output", help="Output configuration file", required=True
    )
    parser.add_argument(
        "--case-name",
        dest="case_name",
        help="Name of case/suite you want to run",
        required=True,
    )
    parser.add_argument(
        "--plugin-home",
        dest="plugin_home",
        help="Path to plugin home directory where surfexp is located",
        required=True,
    )
    parser.add_argument(
        "--start-time",
        dest="start_time",
        type=str,
        help="ISO start time",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--end-time",
        dest="end_time",
        type=str,
        help="ISO end time",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--troika-command",
        dest="troika_command",
        type=str,
        help="Troika command",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--start-suite",
        dest="start_suite",
        action="store_true",
        help="Start suite",
        default=False,
        required=False,
    )

    parser.add_argument(
        "--continue",
        dest="continue_mode",
        action="store_true",
        help="Disable prep and continue run",
        default=False,
        required=False,
    )
    parser.add_argument(
        "args", help="Optional extra input configuration files", nargs="*"
    )
    args = parser.parse_args(argv)

    output = args.output
    case_name = args.case_name
    plugin_home = args.plugin_home
    start_time = args.start_time
    end_time = args.end_time
    start_suite = args.start_suite
    continue_mode = args.continue_mode
    troika_command = args.troika_command
    args = args.args

    deode_path = deode.__path__[0]
    surfexp_path = surfexp.__path__[0]
    tmp_output = f"{output}.tmp.{os.getpid()}.toml"
    tmp_mods_output = f"{output}.mods.tmp.{os.getpid()}.toml"
    argv = [
        "case",
        "--case-name",
        case_name,
        "--config-file",
        f"{deode_path}/data/config_files/config.toml",
        "--config-data-dir",
        f"{surfexp_path}/data/config/",
        "--output",
        tmp_output,
        f"{surfexp_path}/data/surfexp.toml",
    ]

    if troika_command is None:
        troika_command = shutil.which("troika")
    with open(tmp_mods_output, mode="w", encoding="utf8") as fhandler:
        if start_time is not None or end_time is not None:
            fhandler.write("[general.times]\n")
            if start_time is not None:
                fhandler.write(f'  start = "{start_time}"\n')
            if end_time is not None:
                fhandler.write(f'  end = "{end_time}"\n')
        if troika_command != "":
            fhandler.write("[troika]\n")
            fhandler.write(f"  troika = '{troika_command}'\n")
        if continue_mode:
            fhandler.write("[suite_control]\n")
            fhandler.write(" do_prep = false\n")

    argv += args
    argv.append(tmp_mods_output)
    cmd = " ".join(argv)

    logger.debug("deode case command: deode {}", cmd)
    main(argv=argv)
    with open(tmp_output, mode="r", encoding="utf8") as fhandler_in, open(
        output, mode="w", encoding="utf8"
    ) as fhandler_out:
        for lline in fhandler_in.readlines():
            line = lline.replace("@PLUGIN_HOME@", plugin_home)
            fhandler_out.write(line)
    os.remove(tmp_output)
    if os.path.exists(tmp_mods_output):
        os.remove(tmp_mods_output)

    if start_suite:
        argv = ["start", "suite", "--config-file", output]
        cmd = " ".join(argv)
        main(argv=argv)
