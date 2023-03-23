#!/usr/bin/env python3
"""Unit tests for the config file parsing module."""
import logging
import os
from pathlib import Path

import pytest
import surfex

from experiment.datetime_utils import as_datetime
from experiment.experiment import Exp, ExpFromFiles
from experiment.system import System
from experiment.toolbox import FileManager


@pytest.fixture(scope="module")
def sfx_exp_config(tmp_path_factory):
    wdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    exp_name = "test_config"
    pysurfex_experiment = f"{str(((Path(__file__).parent).parent).parent)}"
    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = f"{tmp_path_factory.getbasetemp().as_posix()}/source"

    exp_dependencies = ExpFromFiles.setup_files(
        wdir, exp_name, None, pysurfex, pysurfex_experiment, offline_source=offline_source
    )

    scratch = f"{tmp_path_factory.getbasetemp().as_posix()}"
    env_system = {
        "host_system": {
            "compcentre": "LOCAL",
            "hosts": ["my_host_0", "my_host_1"],
            "sfx_exp_data": f"{scratch}/host0/@EXP@",
            "sfx_exp_lib": f"{scratch}/host0/@EXP@/lib",
            "host_name": "",
            "joboutdir": f"{scratch}/host0/job",
            "hm_cs": "gfortran",
            "parch": "",
            "mkdir": "mkdir -p",
            "rsync": 'rsync -avh -e "ssh -i ~/.ssh/id_rsa"',
            "surfex_config": "my_UNITonie_config",
            "login_host": "localhost",
            "scheduler_pythonpath": "",
            "host1": {
                "sfx_exp_data": f"{scratch}/host1/@EXP@",
                "sfx_exp_lib": f"{scratch}/host1/@EXP@/lib",
                "host_name": "",
                "joboutdir": f"{scratch}/host1/job",
                "login_host": "localhost",
                "sync_data": True,
            },
        }
    }
    system_file_paths = {
        "soilgrid_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}"
    }
    env_submit = {
        "submit_types": ["background", "scalar"],
        "default_submit_type": "scalar",
        "background": {
            "HOST": "0",
            "OMP_NUM_THREADS": 'import os\nos.environ.update({"OMP_NUM_THREADS": "1"})',
            "tasks": ["InitRun", "LogProgress", "LogProgressPP"],
        },
        "scalar": {"HOST": "1", "Not_existing_task": {"DR_HOOK": 'print("Hello world")'}},
    }
    progress = {
        "basetime": "2023-01-01T03:00:00Z",
        "start": "2023-01-01T00:00:00Z",
        "end": "2023-01-01T06:00:00Z",
        "basetime_pp": "2023-01-01T03:00:00Z",
    }

    # Configuration
    config_files_dict = ExpFromFiles.get_config_files(
        exp_dependencies["config"]["config_files"], exp_dependencies["config"]["blocks"]
    )
    merged_config = ExpFromFiles.merge_dict_from_config_dicts(config_files_dict)

    merged_config["general"]["case"] = "mytest"
    merged_config["general"]["os_macros"] = ["USER", "HOME"]
    merged_config["general"]["realization"] = -1
    merged_config["general"]["cnmexp"] = "UNIT"
    merged_config["general"]["tstep"] = 60
    merged_config["general"]["loglevel"] = "DEBUG"
    if "times" not in merged_config["general"]:
        merged_config["general"].update({"times": {}})
    merged_config["general"]["times"]["basetime"] = as_datetime("2000-01-01 T00:00:00Z")
    merged_config["general"]["times"]["validtime"] = as_datetime("2000-01-02 T00:00:00Z")

    # Create Exp/Configuration object
    stream = None
    system = System(env_system, exp_name)
    env_server = {"ECF_HOST": "localhost"}
    sfx_exp = Exp(
        exp_dependencies,
        merged_config,
        system,
        system_file_paths,
        env_server,
        env_submit,
        progress,
        stream=stream,
    )

    update = {
        "general": {
            "case": "mytest",
            "os_macros": ["USER", "HOME"],
            "realization": -1,
            "cnmexp": "UNIT",
            "tstep": 60,
            "loglevel": "DEBUG",
            "times": {
                "basetime": as_datetime("2000-01-01 T00:00:00Z"),
                "validtime": as_datetime("2000-01-02 T00:00:00Z"),
            },
        },
        "system": {
            "wrk": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "bindir": f"{tmp_path_factory.getbasetemp().as_posix()}/bin",
            "archive": f"{tmp_path_factory.getbasetemp().as_posix()}/archive/@YYYY@/@MM@/@DD@/@HH@",
        },
        "platform": {
            "deode_home": "{WORKING_DIR}",
            "scratch": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "static_data": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "climdata": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "prep_input_file": f"{tmp_path_factory.getbasetemp().as_posix()}"
            + "/demo/ECMWF/archive/2023/02/18/18/fc20230218_18+006",
            "soilgrid_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "gmted2010_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}/GMTED2010",
            "namelists": "{WORKING_DIR}/deode/data/namelists",
        },
        "domain": {"name": "DRAMMEN"},
    }
    sfx_exp.config = sfx_exp.config.copy(update=update)
    # Template variables
    update = {
        "task": {
            "args": {
                "check_existence": False,
                "pert": 1,
                "ivar": 1,
                "print_namelist": True,
            }
        }
    }
    config = sfx_exp.config.copy(update=update)
    return config


class TestFileManager:
    # pylint: disable=no-self-use
    """Test FileManager."""

    def test_input_files(self, sfx_exp_config, tmp_path_factory):
        """Test input files."""
        fmanager = FileManager(sfx_exp_config)
        provider, resource = fmanager.get_input(
            "@ARCHIVE@/ICMSH@CNMEXP@+@LLLL@",
            f"{tmp_path_factory.getbasetemp().as_posix()}/ICMSH@CNMEXP@INIT",
            check_archive=True,
        )
        logging.debug("identifier=%s", provider.identifier)
        assert provider.identifier == "ectmp:/2000/01/01/00/ICMSHUNIT+0024"
        assert (
            resource.identifier
            == f"{tmp_path_factory.getbasetemp().as_posix()}/ICMSHUNITINIT"
        )

        os.makedirs(f"{tmp_path_factory.getbasetemp().as_posix()}/bin", exist_ok=True)
        expected_file = tmp_path_factory.getbasetemp() / "bin/MASTERODB"
        expected_file.touch()
        provider, resource = fmanager.get_input(
            "@BINDIR@/MASTERODB", f"{tmp_path_factory.getbasetemp().as_posix()}/MASTERODB"
        )
        assert (
            provider.identifier
            == f"{tmp_path_factory.getbasetemp().as_posix()}/bin/MASTERODB"
        )
        assert (
            resource.identifier
            == f"{tmp_path_factory.getbasetemp().as_posix()}/MASTERODB"
        )
        expected_file = tmp_path_factory.getbasetemp() / "MASTERODB"
        expected_file.touch()
        assert os.path.exists(f"{tmp_path_factory.getbasetemp().as_posix()}/MASTERODB")
        os.remove(f"{tmp_path_factory.getbasetemp().as_posix()}/MASTERODB")
        os.remove(f"{tmp_path_factory.getbasetemp().as_posix()}/bin/MASTERODB")
        os.rmdir(f"{tmp_path_factory.getbasetemp().as_posix()}/bin")

        res_dict = {
            "input": {
                "/dev/null": {
                    "destination": f"{tmp_path_factory.getbasetemp().as_posix()}/test",
                    "provider_id": "symlink",
                }
            }
        }
        fmanager.set_resources_from_dict(res_dict)

    def test_output_files(self, sfx_exp_config, tmp_path_factory):
        """Test input files."""
        fmanager = FileManager(sfx_exp_config)
        os.makedirs(
            f"{tmp_path_factory.getbasetemp().as_posix()}/archive/2000/01/01/00/",
            exist_ok=True,
        )
        expected_file = tmp_path_factory.getbasetemp() / "ICMSHUNIT+0024"
        expected_file.touch()
        provider, aprovider, resource = fmanager.get_output(
            f"{tmp_path_factory.getbasetemp().as_posix()}/ICMSH@CNMEXP@+@LLLL@",
            "@ARCHIVE@/OUT_ICMSH@CNMEXP@+@LLLL@",
            archive=True,
        )
        print(provider)
        print(aprovider)
        print(resource)
        assert (
            resource.identifier
            == f"{tmp_path_factory.getbasetemp().as_posix()}/ICMSHUNIT+0024"
        )
        assert (
            provider.identifier
            == f"{tmp_path_factory.getbasetemp().as_posix()}/archive/2000/01/01/00/OUT_ICMSHUNIT+0024"
        )
        assert os.path.exists(
            f"{tmp_path_factory.getbasetemp().as_posix()}/archive/2000/01/01/00/OUT_ICMSHUNIT+0024"
        )
        assert aprovider.identifier == "ectmp:/2000/01/01/00/OUT_ICMSHUNIT+0024"
        os.remove(
            f"{tmp_path_factory.getbasetemp().as_posix()}/archive/2000/01/01/00/OUT_ICMSHUNIT+0024"
        )

    def test_case_insensitive(self, sfx_exp_config):
        """Test input files."""
        fmanager = FileManager(sfx_exp_config)
        test = fmanager.platform.sub_value("t/@ARCHIVE@/a@T@b", "ARCHIVE", "found")
        assert test == "t/found/a@T@b"
        test = fmanager.platform.sub_value("t/@ARCHIVE@/a@T@b", "archive", "found")
        assert test == "t/found/a@T@b"
        test = fmanager.platform.sub_value("@TA@t/@ARCHIVE@/a@T@", "archive", "found")
        assert test == "@TA@t/found/a@T@"

    def test_substitution(self, sfx_exp_config):
        """Test input files."""
        config = sfx_exp_config
        platform_value = "platform_value"
        test_config = {
            "general": {
                "cnmexp": "UNIT",
                "times": {
                    "basetime": "2023-02-15T01:30:00Z",
                    "validtime": "2023-02-15T03:30:00Z",
                },
            },
            "domain": {"name": "DOMAIN"},
            "system": {"climdir": "my_dir"},
            "platform": {"test": platform_value},
        }
        config = config.copy(update=test_config)
        fmanager = FileManager(config)
        istring = "@TeST@:@CLimDiR@:@domain@:@cnmexp@:@YYYY@:@MM@:@DD@:@HH@:@mm@:@LLLL@"
        ostring = f"{platform_value}:my_dir:DOMAIN:UNIT:2023:02:15:01:30:0002"
        test = fmanager.platform.substitute(istring)
        assert test == ostring
