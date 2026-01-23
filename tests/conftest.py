import os

import pytest
import tomli_w
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
def test_arch():
    my_host = "my_dummy_host"
    os.environ["DEODE_HOST"] = my_host
    return my_host


@pytest.fixture(scope="module")
def scratch_dir(tmp_directory):
    return f"{tmp_directory}/scratch"


@pytest.fixture(scope="module")
def dummy_include_files(test_arch, module_initfile, scratch_dir):
    incdir = f"{PACKAGE_DIRECTORY}/data/config/include"
    os.makedirs(incdir, exist_ok=True)
    incdirs = ["scheduler", "archiving", "platform_paths", "submission"]
    for incdir1 in incdirs:
        os.makedirs(f"{incdir}/{incdir1}", exist_ok=True)
    with open(f"{incdir}/scheduler/ecflow_{test_arch}.toml", mode="wb") as fhandler:
        fdef = {
            "scheduler": {
                "ecfvars": {
                    "case_prefix": "",
                    "ecf_deode_home": "strip_off_mount_path('@DEODE_HOME@',)",
                    "ecf_files": "@HOME@/deode_ecflow/ecf_files",
                    "ecf_files_remotely": "@HOME@/deode_ecflow/ecf_files",
                    "ecf_home": "@HOME@/deode_ecflow/jobout",
                    "ecf_host": "pc5709",
                    "ecf_jobout": "@HOME@/deode_ecflow/jobout",
                    "ecf_out": "@HOME@/deode_ecflow/jobout",
                    "ecf_port": 44855,
                    "troika": {
                        "config_file": "@ECF_DEODE_HOME@/data/config_files/troika.yml"
                    },
                }
            }
        }
        tomli_w.dump(fdef, fhandler)
    with open(f"{incdir}/submission/{test_arch}.toml", mode="wb") as fhandler:
        tomli_w.dump(
            {
                "submission": {
                    "bindir": f"{scratch_dir}/surfexp/bin",
                    "default_submit_type": "serial",
                    "module_initfile": module_initfile,
                    "task": {"wrapper": ""},
                    "types": {
                        "serial": {
                            "NPROC": 1,
                            "NPROCX": 1,
                            "NPROCY": 1,
                            "SCHOST": "localhost",
                            "WRAPPER": "",
                            "BATCH": {},
                            "ENV": {},
                            "MODULES": {},
                        }
                    },
                }
            },
            fhandler,
        )

    with open(f"{incdir}/archiving/{test_arch}.toml", mode="wb") as fhandler:
        tomli_w.dump({"archiving": {}}, fhandler)

    with open(f"{incdir}/platform_paths/{test_arch}.toml", mode="wb") as fhandler:
        tomli_w.dump(
            {
                "platform": {
                    "scratch": scratch_dir,
                    "archive_root": "@SCRATCH@/@CASE@/archive",
                    "albnir_soil_dir": "@CLIMDATA@/ECOCLIMAP-SG/V0/ALB_SAT",
                    "albnir_veg_dir": "@CLIMDATA@/ECOCLIMAP-SG/V0/ALB_SAT",
                    "albvis_soil_dir": "@CLIMDATA@/ECOCLIMAP-SG/V0/ALB_SAT",
                    "albvis_veg_dir": "@CLIMDATA@/ECOCLIMAP-SG/V0/ALB_SAT",
                    "archive_type": "ecfs",
                    "climdata": "@STATIC_DATA@/climate",
                    "deode_home": "set-by-the-system",
                    "e923_data": "@STATIC_DATA@/climate/E923_DATA",
                    "ecoclim_data_path": "@CLIMDATA@/ecoclimap",
                    "ecoclimap_bin_dir": "@ecoclim_data_path@",
                    "ecosg_data_path": "@CLIMDATA@/ECOCLIMAP-SG/V0",
                    "fixed_bdclimdir": "",
                    "fixed_bddir": "",
                    "fixed_bddir_sfx": "",
                    "flake_dir": "@STATIC_DATA@/climate/",
                    "global_sfcdir": "@STATIC_DATA@/climate_fields_mir/climate.v020_MIR_orog/",
                    "lai_dir": "@CLIMDATA@/ECOCLIMAP-SG/V0/LAI_SAT",
                    "ncdir": "@STATIC_DATA@/ncdir",
                    "osm_data": "@CLIMDATA@/OSM_SFX8_1/GARDEN/",
                    "pgd_data_path": "@CLIMDATA@/PGD",
                    "rrtm_dir": "@STATIC_DATA@/rrtm/@CYCLE@",
                    "soilgrid_data_path": "@CLIMDATA@/soilgrid",
                    "static_data": "@HOME@/nobackup/harmonie/",
                    "task_name": "@STAND_ALONE_TASK_NAME@",
                    "tree_height_dir": "@CLIMDATA@/ECOCLIMAP-SG/V0/HT",
                    "unix_group": "",
                    "windfarm_path": "@STATIC_DATA@/WFP_input_files/",
                    "topo_data_path": "@CLIMDATA@/gmted2010",
                },
                "system": {"casedir": "@SCRATCH@/surfexp/@CASE@"},
            },
            fhandler,
        )


@pytest.fixture(scope="module")
def module_initfile(tmp_directory):
    module_initfile = f"{tmp_directory}/module_initfile"
    os.system(f"touch {module_initfile}")  # noqa S605
    return module_initfile


@pytest.fixture(scope="module")
def tactus_config(tmp_directory, dummy_include_files):  # noqa
    output_file = f"{tmp_directory}/config_tactus.toml"

    argv = [
        "-o",
        output_file,
        "--case-name",
        "tactus_case_name",
        "--plugin-home",
        f"{PACKAGE_DIRECTORY}/..",
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
def default_config_file(dummy_include_files):  # noqa
    output_file_static = "static_config.toml"
    if True:
        argv = [
            "-o",
            output_file_static,
            "--case-name",
            "default_case_name",
            "--plugin-home",
            f"{PACKAGE_DIRECTORY}/..",
            f"{PACKAGE_DIRECTORY}/data/config/domains/DRAMMEN.toml",
        ]
        pysfxexp(argv=argv)

    output_file = output_file_static
    return output_file
