import os

import pytest
from tactus.config_parser import ConfigParserDefaults, ParsedConfig
from tactus.derived_variables import set_times
from tactus.logs import logger

from surfexp import PACKAGE_DIRECTORY
from surfexp.cli import pysfxexp


@pytest.fixture(scope="module")
def project_directory():
    os.chdir(f"{PACKAGE_DIRECTORY}/..")


def new_main(argv=None):
    logger.info("argv={}", argv)
    os.system(f"touch {tmp_directory}/out.toml.tmp.{os.getpid()}.toml")  # noqa S605


@pytest.fixture(scope="module")
def tmp_directory(tmp_path_factory):
    """Return a temp directory valid for this module."""
    return tmp_path_factory.getbasetemp().as_posix()


@pytest.fixture(scope="module")
def tactus_config(tmp_directory):
    output_file = f"{tmp_directory}/config_tactus.toml"
    with open(f"{tmp_directory}/mods.toml", mode="w") as fhandler:
        fhandler.write("[platform]\n")
        fhandler.write(f'scratch = "{tmp_directory}"\n')
        fhandler.write('unix_group = "suv"\n')

    argv = [
        "-o",
        output_file,
        "--case-name",
        "tactus_case_name",
        "--plugin-home",
        f"{PACKAGE_DIRECTORY}/..",
        f"{tmp_directory}/mods.toml",
        f"{PACKAGE_DIRECTORY}/data/config/domains/DRAMMEN.toml",
        f"{PACKAGE_DIRECTORY}/data/config/configurations/dt.toml",
        f"{PACKAGE_DIRECTORY}/data/config/mods/dt_an_forcing.toml",
    ]
    pysfxexp(argv=argv)

    config = ParsedConfig.from_file(
        output_file, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA
    )
    config = config.copy(update=set_times(config))
    return config


@pytest.fixture(scope="module")
def default_config(default_config_file):
    config = ParsedConfig.from_file(
        default_config_file, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA
    )
    config = config.copy(update=set_times(config))
    return config


@pytest.fixture(scope="module")
def default_config_file(tmp_directory):
    output_file_static = "/home/trygveasp/projects/surfExp/config.toml"
    if True:
        output_file = f"{tmp_directory}/config_default.toml"
        with open(f"{tmp_directory}/mods.toml", mode="w", encoding="utf8") as fhandler:
            fhandler.write("[platform]\n")
            fhandler.write(f'scratch = "{tmp_directory}"\n')
            fhandler.write('unix_group = "suv"\n')
        argv = [
            "-o",
            output_file,
            "--case-name",
            "default_case_name",
            "--plugin-home",
            f"{PACKAGE_DIRECTORY}/..",
            f"{tmp_directory}/mods.toml",
            f"{PACKAGE_DIRECTORY}/data/config/domains/DRAMMEN.toml",
        ]
        pysfxexp(argv=argv)
        os.system(f"cp {output_file} {output_file_static}")  # noqa S605

    output_file = output_file_static
    return output_file
