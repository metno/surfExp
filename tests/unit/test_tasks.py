#!/usr/bin/env python3
"""Unit tests for the config file parsing module."""
import subprocess
from pathlib import Path

from unittest.mock import patch
from datetime import datetime
import pytest
import tomlkit
import numpy as np

import surfex
from surfex import BatchJob

import experiment
from experiment.progress import Progress
from experiment.tasks.tasks import AbstractTask
from experiment.tasks.discover_tasks import discover, get_task
from experiment.scheduler.scheduler import EcflowServer
from experiment.experiment import ExpFromFiles, Exp

WORKING_DIR = Path.cwd()


def classes_to_be_tested():
    """Return the names of the task-related classes to be tested."""
    encountered_classes = discover(experiment.tasks, AbstractTask,
                                   attrname="__type_name__")
    return encountered_classes.keys()


@pytest.fixture(scope="module")
def get_config(tmp_path_factory):
    wdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    exp_name = "test_config"
    pysurfex_experiment = f"{str(((Path(__file__).parent).parent).parent)}"
    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = "/tmp/source"

    exp_dependencies = ExpFromFiles.setup_files(wdir, exp_name, None, pysurfex,
                                                pysurfex_experiment,
                                                offline_source=offline_source)

    scratch = f"{tmp_path_factory.getbasetemp().as_posix()}"
    env_system = {
        "HOST_SYSTEM": {
            "COMPCENTRE": "LOCAL",
            "HOSTS": ["my_host_0", "my_host_1"],
            "SFX_EXP_DATA": f"{scratch}/host0/@EXP@",
            "SFX_EXP_LIB": f"{scratch}/host0/@EXP@/lib",
            "HOST_NAME": "",
            "JOBOUTDIR": f"{scratch}/host0/job",
            "HM_CS": "gfortran",
            "PARCH": "",
            "MKDIR": "mkdir -p",
            "RSYNC": 'rsync -avh -e \"ssh -i ~/.ssh/id_rsa\"',
            "SURFEX_CONFIG": "my_harmonie_config",
            "LOGIN_HOST": "localhost",
            "SCHEDULER_PYTHONPATH": "",
            "HOST1": {
                "SFX_EXP_DATA": f"{scratch}/host1/@EXP@",
                "SFX_EXP_LIB": f"{scratch}/host1/@EXP@/lib",
                "HOST_NAME": "",
                "JOBOUTDIR": f"{scratch}/host1/job",
                "LOGIN_HOST": "localhost",
                "SYNC_DATA": True
            }
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
            "OMP_NUM_THREADS": "import os\nos.environ.update({\"OMP_NUM_THREADS\": \"1\"})",
            "tasks": [
                "InitRun",
                "LogProgress",
                "LogProgressPP"
            ]
        },
        "scalar": {
            "HOST": "1",
            "Not_existing_task": {
                "DR_HOOK": "print(\"Hello world\")"
            }
        }
    }
    progressObj = Progress(dtg=datetime(year=2023, month=1, day=1, hour=3),
                           dtgbeg=datetime(year=2023, month=1, day=1, hour=0),
                           dtgend=datetime(year=2023, month=1, day=1, hour=6),
                           dtgpp=datetime(year=2023, month=1, day=1, hour=3))
    domains = {
        "DRAMMEN": {
            "GSIZE": 2500.0,
            "LAT0": 60.0,
            "LATC": 60.0,
            "LON0": 10.0,
            "LONC": 10.0,
            "NLAT": 60,
            "NLON": 50,
            "TSTEP": 600,
            "EZONE": 0
        }
    }
    # Configuration
    config_files_dict = ExpFromFiles.get_config_files(exp_dependencies["config"]["config_files"],
                                                      exp_dependencies["config"]["blocks"])
    merged_config = ExpFromFiles.merge_dict_from_config_dicts(config_files_dict)

    merged_config.update({
        "general": {
            "loglevel": "INFO",
            "case": "my_case",
            "realization": -1,
            "os_macros": ["HOME"],
            "platform": "unittest",
            "cnmexp": "",
            "tstep": 60,
            "times": {
                "basetime": "2023-02-19T00:00:00Z",
                "validtime": "2023-02-19T00:00:00Z"
            }
        },
        "system": {
            "wrk": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "bindir": f"{tmp_path_factory.getbasetemp().as_posix()}/bin"
        },
        "platform": {
            "deode_home": "{WORKING_DIR}",
            "scratch": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "static_data": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "climdata": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "prep_input_file": f"{tmp_path_factory.getbasetemp().as_posix()}" +
                               "/demo/ECMWF/archive/2023/02/18/18/fc20230218_18+006",
            "soilgrid_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}",
            "gmted2010_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}/GMTED2010",
            "namelists": "{WORKING_DIR}/deode/data/namelists"
        },
        "domain": {
            "name": "DRAMMEN"
        }
    })
    # Create Exp/Configuration object
    stream = None
    with patch('experiment.scheduler.scheduler.ecflow') as mock_ecflow:
        server = EcflowServer({"ECF_HOST": "localhost"})
        sfx_exp = Exp(exp_dependencies, merged_config, env_system, system_file_paths,
                      server, env_submit, progressObj, domains, stream=stream)

    # Template variables
    sfx_exp.update_setting("TASK#ARGS#check_existence", False)
    sfx_exp.update_setting("TASK#ARGS#pert", 1)
    sfx_exp.update_setting("TASK#ARGS#ivar", 1)
    # force
    # print_namelist
    return sfx_exp


@pytest.fixture(params=classes_to_be_tested())
def task_name_and_configs(request, get_config, tmp_path_factory):
    """Return a ParsedConfig with a task-specific section according to `params`."""
    task_name = request.param
    # task_config = ParsedConfig.parse_obj(base_raw_config, json_schema={})
    task_config = get_config

    config_patch = tomlkit.parse(
        f"""
        [general]
            case = "my_case"
        """
        )

    return task_name, task_config


@pytest.fixture(scope="module")
def _mockers_for_task_run_tests(session_mocker, tmp_path_factory):
    """Define mockers used in the tests for the tasks' `run` methods."""
    # Keep reference to the original methods that will be replaced with wrappers
    original_batchjob_init_method = BatchJob.__init__
    original_batchjob_run_method = BatchJob.run

    # Define the wrappers that will replace some key methods
    def new_batchjob_init_method(self, *args, **kwargs):
        """Remove eventual `wrapper` settings, which are not used for tests."""
        original_batchjob_init_method(self, *args, **kwargs)
        self.wrapper = ""

    def new_write_obsmon_sqlite_file(*args, **kwargs):
        """Run the original method with a dummy cmd if the original cmd fails."""
        pass

    def new_converter(*args, **kwargs):
        pass

    def new_oi2soda(*args, **kwargs):
        pass

    def new_horizontal_oi(*args, **kwargs):
        pass

    def new_get_system_path(*args, **kwargs):
        pass

    def new_read_first_guess_netcdf_file(*args, **kwargs):
        geo_dict = {
            "nam_pgd_grid": {
                "cgrid": "CONF PROJ"
            },
            "nam_conf_proj": {
                "xlat0": 59.5,
                "xlon0": 9
            },
            "nam_conf_proj_grid": {
                "ilone": 0,
                "ilate": 0,
                "xlatcen": 60,
                "xloncen": 10,
                "nimax": 50,
                "njmax": 60,
                "xdx": 2500.0,
                "xdy": 2500.0
            }
        }
        geo = surfex.ConfProj(geo_dict)
        validtime = datetime(year=2023, month=1, day=1, hour=3)
        dummy = np.empty([60, 50])
        return geo, validtime, dummy, dummy, dummy

    def new_write_analysis_netcdf_file(*args, **kwargs):
        pass

    def new_dataset_from_file(*args, **kwargs):
        return {}

    def new_converted_input(*args, **kwargs):
        return np.empty([3000])
        # surfex.read.ConvertedInput

    def new_surfex_binary(*args, **kwargs):
        pass

    def new_batchjob_run_method(self, cmd):
        """Run the original method with a dummy cmd if the original cmd fails."""
        try:
            original_batchjob_run_method(self, cmd=cmd)
        except subprocess.CalledProcessError:
            original_batchjob_run_method(
                self, cmd="echo 'Running a dummy command' >| output"
            )

    # Do the actual mocking
    session_mocker.patch(
        "surfex.BatchJob.__init__", new=new_batchjob_init_method
    )
    session_mocker.patch(
        "surfex.write_obsmon_sqlite_file", new=new_write_obsmon_sqlite_file
    )
    session_mocker.patch(
        "surfex.read.Converter", new=new_converter
    )
    session_mocker.patch(
        "surfex.oi2soda", new=new_oi2soda
    )
    session_mocker.patch(
        "surfex.read.ConvertedInput.read_time_step", new=new_converted_input
    )
    session_mocker.patch(
        "surfex.read_first_guess_netcdf_file", new=new_read_first_guess_netcdf_file
    )
    session_mocker.patch(
        "surfex.write_analysis_netcdf_file", new=new_write_analysis_netcdf_file
    )
    session_mocker.patch(
        "surfex.horizontal_oi", new=new_horizontal_oi
    )
    session_mocker.patch(
        "surfex.PgdInputData", new=new_get_system_path
    )
    session_mocker.patch(
        "surfex.run.PerturbedOffline", new=new_surfex_binary
    )
    session_mocker.patch(
        "surfex.run.SURFEXBinary", new=new_surfex_binary
    )
    session_mocker.patch(
        "surfex.dataset_from_file", new=new_dataset_from_file
    )
    session_mocker.patch("surfex.BatchJob.run", new=new_batchjob_run_method)

    # Create files needed by gmtedsoil tasks
    tif_files_dir = tmp_path_factory.getbasetemp() / "GMTED2010"
    tif_files_dir.mkdir()
    for fname in ["50N000E_20101117_gmted_mea075", "30N000E_20101117_gmted_mea075"]:
        fpath = tif_files_dir / f"{fname}.tif"
        fpath.touch()

    # Mock things that we don't want to test here (e.g., external binaries)
    session_mocker.patch("experiment.tasks.gmtedsoil._import_gdal")
    session_mocker.patch("surfex.SURFEXBinary")


class TestTasks:
    # pylint: disable=no-self-use
    """Test all tasks."""

    def test_task_can_be_instantiated(self, task_name_and_configs):
        class_name, task_config = task_name_and_configs
        assert isinstance(get_task(class_name, task_config), AbstractTask)

    @pytest.mark.usefixtures("_mockers_for_task_run_tests")
    def test_task_can_be_run(self, task_name_and_configs):
        class_name, task_config = task_name_and_configs
        my_task_class = get_task(class_name, task_config)
        my_task_class.var_name = "t2m"
        my_task_class.fc_start_sfx = f"{my_task_class.fc_start_sfx}_{class_name}"
        my_task_class.run()
