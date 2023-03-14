#!/usr/bin/env python3
"""Unit tests for the config file parsing module."""
import pytest
import tomlkit

#from deode.config_parser import ParsedConfig

from unittest.mock import patch
from pathlib import Path
from datetime import datetime
import experiment
import surfex
import experiment_scheduler
from experiment_scheduler import NoSchedulerSubmission, TaskSettings


@pytest.fixture(scope="module")
def config(tmp_path_factory):
    wdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    exp_name = "test_config"
    pysurfex_experiment = f"{str(((Path(__file__).parent).parent).parent)}"
    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = "/tmp/source"

    exp_dependencies = experiment.ExpFromFiles.setup_files(wdir, exp_name, None, pysurfex,
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
    system_file_paths = {}
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
    progressObj = experiment.Progress(dtg=datetime(year=2023, month=1, day=1, hour=3),
                                      dtgbeg=datetime(year=2023, month=1, day=1, hour=0),
                                      dtgend=datetime(year=2023, month=1, day=1, hour=6),
                                      dtgpp=datetime(year=2023, month=1, day=1, hour=3))
    domains = {
        "DRAMMEN": {
            "GSIZE": 2500.0,
            "LAT0": 60.0,
            "LATC": 60.0 ,
            "LON0": 10.0,
            "LONC": 10.0,
            "NLAT": 60,
            "NLON": 50,
            "TSTEP": 600,
            "EZONE": 0
        }
    }
    # Configuration
    config_files_dict = experiment.ExpFromFiles.get_config_files(exp_dependencies["config"]["config_files"],
                                                                 exp_dependencies["config"]["blocks"])
    merged_config = experiment.ExpFromFiles.merge_dict_from_config_dicts(config_files_dict)

    # Create Exp/Configuration object
    stream = None
    with patch('experiment_scheduler.scheduler.ecflow') as mock_ecflow:
        server = experiment_scheduler.scheduler.EcflowServer({"ECF_HOST": "localhost"})
        sfx_exp = experiment.Exp(exp_dependencies, merged_config, env_system, system_file_paths,
                     server, env_submit, progressObj, domains, stream=stream)
        

    # Template variables
    sfx_exp.update_setting("TASK#ARGS#check_existence", False)
    sfx_exp.update_setting("TASK#ARGS#pert", 1)
    sfx_exp.update_setting("TASK#ARGS#ivar", 1)
    # force
    # print_namelist
    return sfx_exp


class TestSubmission:
    # pylint: disable=no-self-use

    def test_submit(self, config, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        config.update_setting("submission", {
            "submit_types": ["unittest"],
            "default_submit_type": "unittest",
            "unittest": {
                "SCHOST": "localhost"
            }
        })
        task = "PrepareCycle"
        template_job = f"{config.scripts}/ecf/stand_alone.py"
        task_job = f"{tmpdir}/{task}.job"
        output = f"{tmpdir}/{task}.log"

        assert config.get_setting("submission#default_submit_type") == "unittest"
        background = TaskSettings(config)
        sub = NoSchedulerSubmission(background)
        sub.submit(
            task, config, template_job, task_job, output
        )

    def test_get_batch_info(self, config):
        arg = "#SBATCH UNITTEST"
        config.update_setting("submission", {
            "submit_types": ["unittest"],
            "default_submit_type": "unittest",
            "unittest": {
                "BATCH": {
                    "TEST": arg
                }
            }
        })
        task = TaskSettings(config)
        settings = task.get_task_settings("unittest", key="BATCH")
        assert settings["TEST"] == arg

    def test_get_batch_info_exception(self, config):
        arg = "#SBATCH UNITTEST"
        config.update_setting("submission", {
            "submit_types": ["unittest"],
            "default_submit_type": "unittest",
            "unittest": {
                "tasks": ["unittest"],
                "BATCH": {
                    "TEST_INCLUDED": arg,
                    "TEST": "NOT USED"
                }
            },
            "task_exceptions": {
                "unittest": {
                    "BATCH": {
                        "TEST": arg
                    }
                }
            }
        })
        task = TaskSettings(config)
        settings = task.get_task_settings("unittest", key="BATCH")
        assert settings["TEST"] == arg
        assert settings["TEST"] != "NOT USED"
        assert settings["TEST_INCLUDED"] == arg

    def test_submit_non_existing_task(self, config, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        config.update_setting("submission", {
            "submit_types": ["unittest"],
            "default_submit_type": "unittest",
            "unittest": {
                "SCHOST": "localhost"
            }
        })
        task = "not_existing"
        template_job = "ecf/stand_alone.py"
        task_job = f"{tmpdir}/{task}.job"
        output = f"{tmpdir}/{task}.log"

        background = TaskSettings(config)
        sub = NoSchedulerSubmission(background)
        with pytest.raises(Exception, match="Task not found:"):
            sub.submit(
                task,
                config,
                template_job,
                task_job,
                output
            )
