#!/usr/bin/env python3
"""Unit tests for the config file parsing module."""
from pathlib import Path

import pytest
import surfex

from experiment.config_parser import ParsedConfig
from experiment.experiment import Exp, ExpFromFiles
from experiment.scheduler.submission import NoSchedulerSubmission, TaskSettings
from experiment.system import System


@pytest.fixture(scope="module")
def config(tmp_path_factory):
    wdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    exp_name = "test_config"
    pysurfex_experiment = f"{str(((Path(__file__).parent).parent).parent)}"
    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = f"{wdir}/source"

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
            "surfex_config": "my_harmonie_config",
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

    system = System(env_system, exp_name)
    system_file_paths = {
        "soilgrid_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "ecoclimap_bin_dir": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "ecosg_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "pgd_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "scratch": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "static_data": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "climdata": f"{tmp_path_factory.getbasetemp().as_posix()}",
        "prep_input_file": f"{tmp_path_factory.getbasetemp().as_posix()}"
        + "/demo/ECMWF/archive/2023/02/18/18/fc20230218_18+006",
        "gmted2010_data_path": f"{tmp_path_factory.getbasetemp().as_posix()}/GMTED2010",
        "namelists": "{WORKING_DIR}/deode/data/namelists",
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

    # Create Exp/Configuration object
    stream = None
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

    config_file = f"{tmp_path_factory.getbasetemp().as_posix()}/config.json"
    sfx_exp.dump_json(config_file)
    config = ParsedConfig.from_file(config_file)
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
    config = config.copy(update=update)
    return config


@pytest.fixture(scope="module")
def _mockers_for_submission(session_mocker):
    session_mocker.patch("experiment.scheduler.submission.TaskSettings.parse_job")


class TestSubmission:
    # pylint: disable=no-self-use

    @pytest.mark.usefixtures("_mockers_for_submission")
    def test_submit(self, config, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        update = {
            "submission": {
                "submit_types": ["unittest"],
                "default_submit_type": "unittest",
                "unittest": {"SCHOST": "localhost"},
            }
        }
        config = config.copy(update=update)
        task = "preparecycle"
        pysurfex_experiment = config.get_value("system.pysurfex_experiment")
        template_job = f"{pysurfex_experiment}/experiment/templates/stand_alone.py"
        task_job = f"{tmpdir}/{task}.job"
        output = f"{tmpdir}/{task}.log"

        assert config.get_value("submission.default_submit_type") == "unittest"
        background = TaskSettings(config)
        sub = NoSchedulerSubmission(background)
        with pytest.raises(RuntimeError):
            sub.submit(task, config, template_job, task_job, output)

    def test_get_batch_info(self, config):
        arg = "#SBATCH UNITTEST"
        update = {
            "submission": {
                "submit_types": ["unittest"],
                "default_submit_type": "unittest",
                "unittest": {"BATCH": {"TEST": arg}},
            }
        }
        config = config.copy(update=update)
        task = TaskSettings(config)
        settings = task.get_task_settings("unittest", key="BATCH")
        assert settings["TEST"] == arg

    def test_get_batch_info_exception(self, config):
        arg = "#SBATCH UNITTEST"
        update = {
            "submission": {
                "submit_types": ["unittest"],
                "default_submit_type": "unittest",
                "unittest": {
                    "tasks": ["unittest"],
                    "BATCH": {"TEST_INCLUDED": arg, "TEST": "NOT USED"},
                },
                "task_exceptions": {"unittest": {"BATCH": {"TEST": arg}}},
            }
        }
        config = config.copy(update=update)
        task = TaskSettings(config)
        settings = task.get_task_settings("unittest", key="BATCH")
        assert settings["TEST"] == arg
        assert settings["TEST"] != "NOT USED"
        assert settings["TEST_INCLUDED"] == arg

    def test_submit_non_existing_task(self, config, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        update = {
            "submission": {
                "submit_types": ["unittest"],
                "default_submit_type": "unittest",
                "unittest": {"SCHOST": "localhost"},
            }
        }
        config = config.copy(update=update)
        task = "not_existing"
        template_job = "ecf/stand_alone.py"
        task_job = f"{tmpdir}/{task}.job"
        output = f"{tmpdir}/{task}.log"

        background = TaskSettings(config)
        sub = NoSchedulerSubmission(background)
        with pytest.raises(Exception, match="Task not found:"):
            sub.submit(task, config, template_job, task_job, output)
