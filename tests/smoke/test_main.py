#!/usr/bin/env python3
"""Smoke tests."""
import os
import itertools
import shutil
from contextlib import redirect_stderr, redirect_stdout, suppress
from io import StringIO
from pathlib import Path
from unittest import mock

import pytest

from experiment import PACKAGE_NAME
from experiment.cli import parse_surfex_script, parse_submit_cmd_exp, \
    parse_update_config, surfex_script, submit_cmd_exp, update_config, surfex_exp
from experiment.setup.setup import surfex_exp_setup

from experiment.scheduler.submission import NoSchedulerSubmission, TaskSettings

WORKING_DIR = Path.cwd()


@pytest.fixture(scope="module")
def config_path(tmp_path_factory):
    config_path = tmp_path_factory.getbasetemp() / "config.toml"
    shutil.copy(WORKING_DIR / "config/config.toml", config_path)
    return config_path


@pytest.fixture(scope="module")
def _module_mockers(session_mocker, config_path, tmp_path_factory):
    
    original_no_scheduler_submission_submit_method = NoSchedulerSubmission.submit
    original_submission_task_settings_parse_job = TaskSettings.parse_job

    def new_no_scheduler_submission_submit_method(*args, **kwargs):
        """Wrap the original method to catch ."""
        with suppress(RuntimeError):
            original_no_scheduler_submission_submit_method(*args, **kwargs)

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

def test_package_executable_is_in_path():
    assert shutil.which("PySurfexExp")

'''
@pytest.mark.parametrize("argv", [[], None])
def test_cannot_run_without_arguments(argv):
    with redirect_stderr(StringIO()):
        with pytest.raises(SystemExit, match="2"):
            parse_surfex_script(argv)

@pytest.mark.usefixtures("_module_mockers")
def test_correct_config_is_in_use(config_path, mocker):
    mocker.patch("sys.exit")
    args = get_parsed_args(argv=[])
    assert args.config_file == config_path


@pytest.mark.usefixtures("_module_mockers")
class TestMainShowCommands:
    # pylint: disable=no-self-use
    def test_show_config_command(self):
        with redirect_stdout(StringIO()):
            main(["show", "config"])

    def test_show_config_command_stretched_time(self):
        """Test again, mocking time.time so the total elapsed time is greater than 60s."""

        def fake_time():
            for new in itertools.count():
                yield 100 * new

        with mock.patch("time.time", mock.MagicMock(side_effect=fake_time())):
            with redirect_stdout(StringIO()):
                main(["show", "config"])


'''

@pytest.fixture(scope="module")
def pysurfex_experiment():
    return f"{str(((Path(__file__).parent).parent).parent)}"

@pytest.fixture(scope="module")
def setup_experiment(tmp_path_factory, pysurfex_experiment):

    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    os.chdir(tmpdir)
    surfex_exp_setup(
        [
            "-experiment",
            pysurfex_experiment,
            "-host",
            "ECMWF-atos",
            "--debug"
        ]
    )

@pytest.mark.usefixtures("_module_mockers")
def test_run_task_command(tmp_path_factory, setup_experiment):

    setup_experiment
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    os.chdir(tmpdir)
    surfex_exp(
        [
            "start",
            "-dtg",
            "202301010300",
            "-dtgend",
            "202301010600",
            "--debug"
        ]
    )


'''
@pytest.mark.usefixtures("_module_mockers")
def test_start_suite_command(tmp_path):
    main(
        [
            "start",
            "suite",
            "--joboutdir",
            tmp_path.as_posix(),
            "--ecf_files",
            f"{WORKING_DIR.as_posix()}/ecf",
        ]
    )
'''
