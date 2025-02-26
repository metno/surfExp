"""Command line interface"""
import argparse
import os
import sys

import deode
import pysurfex
from deode.__main__ import main
from deode.logs import logger

import surfexp


def pysfxexp(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("output")
    parser.add_argument("case_name")
    parser.add_argument("plugin_home")
    parser.add_argument("args", nargs="*")
    args = parser.parse_args(argv)

    output = args.output
    case_name = args.case_name
    plugin_home = args.plugin_home
    args = args.args

    deode_path = deode.__path__[0]
    pysurfex_path = pysurfex.__path__[0]
    surfexp_path = surfexp.__path__[0]
    tmp_output = f"{output}.tmp.{os.getpid()}.toml"
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
        f"{pysurfex_path}/cfg/config_exp_surfex.toml",
        f"{surfexp_path}/data/surfexp.toml",
    ]
    argv += args
    cmd = " ".join(argv)
    logger.debug("deode case command: deode {}", cmd)
    main(argv=argv)
    with open(tmp_output, mode="r", encoding="utf8") as fhandler_in:
        with open(output, mode="w", encoding="utf8") as fhandler_out:
            for line in fhandler_in.readlines():
                line = line.replace("@PLUGIN_HOME@", plugin_home)
                fhandler_out.write(line)
    os.remove(tmp_output)
