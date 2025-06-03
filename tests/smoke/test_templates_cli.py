import json

import pytest

from surfexp.templates.cli import execute_task


@pytest.fixture
def templates_mocker(session_mocker):
    session_mocker.patch("surfexp.templates.stand_alone.get_task")
    session_mocker.patch("surfexp.templates.ecflow.default.get_task")
    session_mocker.patch("surfexp.templates.ecflow.default.EcflowServer")


@pytest.fixture(name="template_args_ecflow")
def fixture_template_args_ecflow(tmp_directory, default_config_file):
    fname = f"{tmp_directory}/template_args_ecflow.json"
    data = {
        "template": "ecflow",
        "ECF_HOST": "my_host",
        "ECF_PORT": "3141",
        "ECF_NAME": "ECF_NAME",
        "ECF_PASS": "ECF_PASS",
        "ECF_TRYNO": "1",
        "ECF_RID": "2",
        "ECF_TIMEOUT": "0",
        "BASETIME": "2025-02-09T00:00:00Z",
        "VALIDTIME": "2025-02-09T00:00:00Z",
        "LOGLEVEL": "INFO",
        "ARGS": "arg1=val1;",
        "WRAPPER": "time",
        "CONFIG": default_config_file,
        "DEODE_HOME": "DEODE_HOME",
    }
    with open(fname, mode="w", encoding="utf8") as fhandler:
        json.dump(data, fhandler)
    return fname


@pytest.mark.usefixtures("templates_mocker")
def test_execute_task_ecflow(template_args_ecflow):
    argv = [template_args_ecflow]
    execute_task(argv=argv)


@pytest.fixture(name="template_args_stand_alone")
def fixture_template_args_stand_alone(tmp_directory, default_config_file):
    fname = f"{tmp_directory}/template_args_stand_alone.json"
    data = {
        "template": "stand_alone",
        "BASETIME": "2025-02-09T00:00:00Z",
        "VALIDTIME": "2025-02-09T00:00:00Z",
        "LOGLEVEL": "INFO",
        "ARGS": "arg1=val1;",
        "WRAPPER": "time",
        "STAND_ALONE_TASK_CONFIG": default_config_file,
        "STAND_ALONE_DEODE_HOME": "DEODE_HOME",
        "STAND_ALONE_TASK_NAME": "task_name",
    }
    with open(fname, mode="w", encoding="utf8") as fhandler:
        json.dump(data, fhandler)
    return fname


@pytest.mark.usefixtures("templates_mocker")
def test_execute_task_stand_alone(template_args_stand_alone):
    argv = [template_args_stand_alone]
    execute_task(argv=argv)
