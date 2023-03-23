"""Unit testing."""
import json
from pathlib import Path

import pytest
import surfex

from experiment.config_parser import ParsedConfig
from experiment.datetime_utils import as_datetime
from experiment.experiment import ExpFromFiles
from experiment.scheduler.scheduler import EcflowServer, EcflowTask
from experiment.scheduler.submission import TaskSettings
from experiment.scheduler.suites import EcflowSuite, EcflowSuiteFamily, EcflowSuiteTask
from experiment.suites import SurfexSuite

TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
ROOT = f"{str((Path(__file__).parent).parent)}"


@pytest.fixture(scope="module")
def ecf_task(tmp_path_factory):
    ecf_name = "/suite/ecf_name"
    ecf_tryno = 2
    ecf_pass = "12345"  # noqa S108
    ecf_rid = 54321
    ecf_timeout = 20
    task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, ecf_timeout=ecf_timeout)
    return task


@pytest.fixture(scope="module")
def get_exp_from_files(tmp_path_factory):
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    wdir = f"{tmpdir}/test_config"
    exp_name = "test_config"
    host = "ECMWF-atos"
    pysurfex_experiment = f"{str(((Path(__file__).parent).parent).parent)}"
    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = f"{tmpdir}/source"
    exp_dependencies = ExpFromFiles.setup_files(
        wdir, exp_name, host, pysurfex, pysurfex_experiment, offline_source=offline_source
    )
    stream = None
    sfx_exp = ExpFromFiles(exp_dependencies, stream=stream)

    exp_configuration_file = f"{tmpdir}/exp_configuration.json"
    sfx_exp.dump_json(exp_configuration_file)
    return ParsedConfig.from_file(exp_configuration_file)


@pytest.fixture(scope="module")
def _mockers_for_ecflow(session_mocker):
    session_mocker.patch("experiment.scheduler.scheduler.Client")
    session_mocker.patch("experiment.scheduler.scheduler.State")
    session_mocker.patch("experiment.scheduler.suites.Defs")
    session_mocker.patch("experiment.scheduler.suites.Defstatus")
    session_mocker.patch("experiment.scheduler.submission.TaskSettings.parse_job")


class TestFlow:
    """Test config."""

    def test_submit(self):
        pass

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_start_server(self):
        """Start the ecflow server."""
        ecf_host = "localhost"
        server = EcflowServer(ecf_host, ecf_port=3141, start_command=None)
        server.start_server()

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_server_from_file(self, tmp_path_factory):
        """Start the ecflow server from a file definition."""
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        server_file = f"{tmpdir}/ecflow_server"
        with open(server_file, mode="w", encoding="utf-8") as server_fh:
            settings = {"ecf_host": "localhost", "ecf_port": 41, "ecf_offset": 3100}
            json.dump(settings, server_fh)
        server = EcflowServer(server_file)
        assert server.ecf_port == 3141

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_begin_suite(self, ecf_task):
        """Begin the suite."""
        ecf_host = "localhost"
        server = EcflowServer(ecf_host, ecf_port=3141, start_command=None)
        server.begin_suite(ecf_task)

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_force_complete(self, ecf_task):
        """Begin the suite."""
        ecf_host = "localhost"
        server = EcflowServer(ecf_host, ecf_port=3141, start_command=None)
        server.force_complete(ecf_task)

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_force_aborted(self, ecf_task):
        """Force task aborted."""
        ecf_host = "localhost"
        server = EcflowServer(ecf_host, ecf_port=3141, start_command=None)
        server.force_aborted(ecf_task)

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_replace(self):
        """Replace the suite."""
        ecf_host = "localhost"
        server = EcflowServer(ecf_host, ecf_port=3141, start_command=None)
        server.replace("suite", "/dev/null")

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_ecflow_task(self):
        """Test the ecflow task wrapper."""
        ecf_name = "/suite/ecf_name"
        ecf_tryno = 2
        ecf_pass = "12345"  # noqa S105
        ecf_rid = 54321
        ecf_timeout = 20
        task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, ecf_timeout=ecf_timeout)
        assert ecf_name == task.ecf_name
        assert ecf_tryno == task.ecf_tryno
        assert ecf_pass == task.ecf_pass
        assert ecf_rid == task.ecf_rid
        assert ecf_timeout == task.ecf_timeout

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_suite(self):
        pass

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_ecflow_suite_task(self, tmp_path_factory, get_exp_from_files):
        """Create a ecflow suite/family/task structure and create job."""
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        ecf_files = f"{tmpdir}"
        suite_name = "suite"
        suite = EcflowSuite(suite_name, ecf_files)
        family_name = "family"
        family = EcflowSuiteFamily(family_name, suite, ecf_files)
        task_name = "task"
        config = get_exp_from_files
        task_settings = TaskSettings(config)
        input_template = f"{ROOT}/experiment/templates/stand_alone.py"
        EcflowSuiteTask(
            task_name,
            family,
            config,
            task_settings,
            ecf_files,
            input_template=input_template,
            parse=True,
            variables=None,
            ecf_micro="%",
            triggers=None,
            def_status=None,
        )

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_ecflow_sufex_suite(self, tmp_path_factory, get_exp_from_files):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        suite_name = "suite"
        joboutdir = f"{tmpdir}"
        config = get_exp_from_files
        task_settings = TaskSettings(config)
        dtg1 = as_datetime("2022-01-01 T03:00:00Z")
        dtg2 = as_datetime("2022-01-01 T06:00:00Z")
        dtgs = [dtg1, dtg2]
        dtgbeg = dtg1
        SurfexSuite(
            suite_name,
            config,
            joboutdir,
            task_settings,
            dtgs,
            dtgbeg=dtgbeg,
            ecf_micro="%",
        )
