"""Command line interface"""
import argparse
import os
import shutil
import subprocess
import sys

import deode
import pysurfex
from deode.__main__ import main

import surfexp


def pysfxexp(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("output")
    parser.add_argument("case_name")
    parser.add_argument("args", nargs="*")
    args = parser.parse_args(argv)

    output = args.output
    case_name = args.case_name
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
        "--output",
        tmp_output,
        f"{pysurfex_path}/cfg/config_exp_surfex.toml",
        f"{surfexp_path}/data/surfexp.toml",
    ]
    argv += args
    main(argv=argv)
    shutil.move(tmp_output, output)
