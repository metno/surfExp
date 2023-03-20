#!/usr/bin/env python3
"""Unit tests for the config file parsing module."""
import pytest

from unittest.mock import patch
from pathlib import Path
import surfex


from experiment.experiment import ExpFromFiles, Exp
from experiment.progress import Progress
from experiment.scheduler.scheduler import EcflowServer
from experiment.scheduler.submission import NoSchedulerSubmission, TaskSettings
from experiment.datetime_utils import as_datetime
from experiment.system import System


@pytest.fixture(scope="module")
def config(tmp_path_factory):
    wdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    exp_name = "test_config"
    pysurfex_experiment = f"{str(((Path(__file__).parent).parent).parent)}"
    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = f"{tmp_path_factory.getbasetemp().as_posix()}/source"

    exp_dependencies = ExpFromFiles.setup_files(wdir, exp_name, None, pysurfex,
                                                pysurfex_experiment,
                                                offline_source=offline_source)

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
            "rsync": 'rsync -avh -e \"ssh -i ~/.ssh/id_rsa\"',
            "surfex_config": "my_harmonie_config",
            "login_host": "localhost",
            "scheduler_pythonpath": "",
            "host1": {
                "sfx_exp_data": f"{scratch}/host1/@EXP@",
                "sfx_exp_lib": f"{scratch}/host1/@EXP@/lib",
                "host_name": "",
                "joboutdir": f"{scratch}/host1/job",
                "login_host": "localhost",
                "sync_data": True
            }
        }
    }
    system = System(env_system, exp_name)
    system_file_paths = {
        "sand_dir": "/tmp/host1/testdata/input_paths/sand_dir",
        "clay_dir": "/tmp/host1/testdata/input_paths/clay_dir",
        "soc_top_dir": "/tmp/host1/testdata/input_paths/soc_top_dir",
        "soc_sub_dir": "/tmp/host1/testdata/input_paths/soc_sub_dir",
        "flake_dir": "/tmp/host1/testdata/input_paths/flake_dir",
        "ecoclimap_dir": "/tmp/host1/testdata/input_paths/ecoclimap_dir",
        "ecoclimap_cover_dir": "/tmp/host1/testdata/input_paths/ecoclimap_cover_dir",
        "ecoclimap_bin_dir": "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir",
        "oro_dir": "/tmp/host1/testdata/input_paths/oro_dir",
        "obs_dir": "/tmp/host1/testdata/"
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
    progress = Progress(dtg=as_datetime("2023-01-01 T03:00:00Z"),
                        dtgbeg=as_datetime("2023-01-01 T00:00:00Z"),
                        dtgend=as_datetime("2023-01-01 T06:00:00Z"),
                        dtgpp=as_datetime("2023-01-01 T03:00:00Z"))
   
    # Configuration
    config_files_dict = ExpFromFiles.get_config_files(exp_dependencies["config"]["config_files"],
                                                      exp_dependencies["config"]["blocks"])
    merged_config = ExpFromFiles.merge_dict_from_config_dicts(config_files_dict)

    # Create Exp/Configuration object
    stream = None
    with patch('experiment.scheduler.scheduler.ecflow') as mock_ecflow:
        server = EcflowServer({"ECF_HOST": "localhost"})
        sfx_exp = Exp(exp_dependencies, merged_config, system, system_file_paths,
                      server, env_submit, progress=progress, stream=stream)

    # Template variables
    update = {
        "task": {
            "wrapper": "time",
            "args": {
                "check_existence": False,
                "pert": 1,
                "ivar": 1        
            }
        }
    }
    return sfx_exp.config.copy(update=update)


class TestSubmission:
    # pylint: disable=no-self-use

    def test_submit(self, config, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        update = {
            "submission": {
                "submit_types": ["unittest"],
                "default_submit_type": "unittest",
                "unittest": {
                    "SCHOST": "localhost"
                }
            }
        }
        config = config.copy(update=update)
        task = "preparecycle"
        template_job = f"{config.get_value('system.pysurfex_experiment')}/experiment/templates/stand_alone.py"
        task_job = f"{tmpdir}/{task}.job"
        output = f"{tmpdir}/{task}.log"

        assert config.get_value("submission.default_submit_type") == "unittest"
        background = TaskSettings(config)
        sub = NoSchedulerSubmission(background)
        sub.submit(
            task, config, template_job, task_job, output
        )

    def test_get_batch_info(self, config):
        arg = "#SBATCH UNITTEST"
        update = {
            "submission": {
                "submit_types": ["unittest"],
                "default_submit_type": "unittest",
                "unittest": {
                    "BATCH": {
                        "TEST": arg
                    }
                }
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
                "unittest": {
                    "SCHOST": "localhost"
                }
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
            sub.submit(
                task,
                config,
                template_job,
                task_job,
                output
            )
