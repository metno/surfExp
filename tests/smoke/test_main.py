#!/usr/bin/env python3
"""Smoke tests."""
import os
import shutil
from contextlib import suppress
from pathlib import Path
import json
import toml

import pytest

from experiment.cli import update_config, surfex_exp, run_submit_cmd_exp
from experiment.setup.setup import surfex_exp_setup

from experiment.scheduler.submission import NoSchedulerSubmission, TaskSettings

WORKING_DIR = Path.cwd()

@pytest.fixture(scope="module")
def create_unit_test_files(tmp_path_factory):
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}/exp/config/"
    system_files = {
        "submit/unittest.json": {
            "json": {
               "submit_types": ["background", "scalar"],
               "default_submit_type": "background",
               "background": {
                    "HOST": "0",
                    "SCHOST": "localhost",
                    "tasks": [
                        "UnitTest", "PrepareCycle"
                    ]
                },
                "scalar": {
                }
            }
        },
        "system/unittest.toml": {
            "toml": {
                "host_system": {
                    "compcentre": "LOCAL",
                    "hosts": ["my_host_0", "my_host_1"],
                    "sfx_exp_data": f"{tmpdir}/sfx_home/@case@",
                    "sfx_exp_lib": f"{tmpdir}/sfx_home/@case@/lib",
                    "host_name": "",
                    "joboutdir": "/tmp/host0/job",
                    "hm_cs": "gfortran",
                    "parch": "",
                    "mkdir":"mkdir -p",
                    "rsync": "rsync -avh -e \"ssh -i ~/.ssh/id_rsa\"",
                    "surfex_config": "my_harmonie_config",
                    "login_host": "localhost",
                    "scheduler_pythonpath": ""
                }
            }
        },
        "server/unittest.json": {
            "json": {
                "ecf_host": "localhost",
                "ecf_port": 3141,
                "ecf_port_offset": 0
            }
        },
        "env/unittest.py": {
            "ascii": {}
        },
        "input_paths/unittest.json": {
            "json": {}
        }
    }
    for fname, ftype_data in system_files.items():
        for ftype, data in ftype_data.items():
            full_name = tmpdir + fname
            os.makedirs(os.path.dirname(full_name), exist_ok=True)
            if ftype == "json":
                json.dump(data, open(full_name, mode="w", encoding="utf8"))
            elif ftype == "toml":
                toml.dump(data, open(full_name, mode="w", encoding="utf8"))
            elif ftype == "ascii":
                os.system(f"touch {full_name}")
            else:
                raise NotImplementedError


@pytest.fixture(scope="module")
def _module_mockers(session_mocker, tmp_path_factory):

    # original_no_scheduler_submission_submit_method = NoSchedulerSubmission.submit
    original_submission_task_settings_parse_job = TaskSettings.parse_job

    def new_no_scheduler_submission_submit_method(self, task, config, template_job,
                                                  task_job, output):
        pass
        """Wrap the original method to catch ."""
        #with suppress(RuntimeError):
        #    original_no_scheduler_submission_submit_method(self, task, config, template_job,
        #                                                   task_job, output)

    def new_submission_task_settings_parse_job(self, task, config, input_template_job, task_job, **kwargs):
        
        task_job = (tmp_path_factory.getbasetemp() / "task_job.txt").as_posix()
        original_submission_task_settings_parse_job(self,  task, config, input_template_job, task_job, **kwargs)

    session_mocker.patch(
        "experiment.scheduler.submission.NoSchedulerSubmission.submit",
        new=new_no_scheduler_submission_submit_method,
    )
    session_mocker.patch("experiment.scheduler.scheduler.ecflow")
    session_mocker.patch("experiment.scheduler.suites.ecflow")
    session_mocker.patch(
        "experiment.scheduler.submission.TaskSettings.parse_job",
        new=new_submission_task_settings_parse_job,
    )

@pytest.fixture(scope="module")
def pysurfex_experiment():
    return f"{str(((Path(__file__).parent).parent).parent)}"

@pytest.fixture(scope="module")
def setup_experiment(tmp_path_factory, pysurfex_experiment, create_unit_test_files):

    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}/exp"
    os.makedirs(tmpdir, exist_ok=True)
    os.chdir(tmpdir)
    create_unit_test_files
    surfex_exp_setup(
        [
            "-experiment",
            pysurfex_experiment,
            "-host",
            "unittest",
            "--debug"
        ]
    )
    return tmpdir + "/exp_dependencies.json"

@pytest.fixture(scope="module")
def update_config_command(tmp_path_factory, setup_experiment):

    __ = setup_experiment
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}/exp"
    os.chdir(tmpdir)
    update_config()
    return tmpdir + "/exp_configuration.json"

def test_pysurfex_exp_executable_is_in_path():
    assert shutil.which("PySurfexExp")

def test_pysurfex_exp_setup_executable_is_in_path():
    assert shutil.which("PySurfexExpSetup")

def test_pysurfex_exp_config_executable_is_in_path():
    assert shutil.which("PySurfexExpConfig")

def test_submit_task_executable_is_in_path():
    assert shutil.which("SubmitTask")

@pytest.mark.usefixtures("_module_mockers")
def test_run_setup_command(setup_experiment):
    __ = setup_experiment

@pytest.mark.usefixtures("_module_mockers")
def test_run_suite_command(tmp_path_factory, setup_experiment):
    __ = setup_experiment
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}/exp"
    os.chdir(tmpdir)
    surfex_exp(
        [
            "start",
            "-dtg",
            "2023-01-01T03:00:00",
            "-dtgend",
            "2023-01-01T06:00:00",
            "--debug"
        ]
    )

@pytest.mark.usefixtures("_module_mockers")
def test_run_submit_task_command(tmp_path_factory, setup_experiment, update_config_command):
    __ = setup_experiment
    exp_config = update_config_command
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}/exp"
    os.makedirs(tmpdir, exist_ok=True)
    os.chdir(tmpdir)
    job = tmpdir + "/Task.job"
    log = tmpdir + "/Task.log"
    template = WORKING_DIR.as_posix() + "/experiment/templates/stand_alone.py"

    run_submit_cmd_exp(
        [
            "-config",
            exp_config,
            "-task",
            "PrepareCycle",
            "-task_job",
            job,
            "-output",
            log,
            "-template",
            template,
            "--debug"
        ]
    )

@pytest.mark.usefixtures("_module_mockers")
def test_update_config_command(setup_experiment, update_config_command):
    __ = setup_experiment
    __ = update_config_command
