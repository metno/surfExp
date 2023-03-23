#!/usr/bin/env python3
"""Unit tests for the config file parsing module."""

import pytest

from experiment.scheduler.scheduler import EcflowClient, EcflowServer, EcflowTask


def suite_name():
    return "test_suite"


@pytest.fixture()
def ecflow_task():
    ecf_name = f"/{suite_name}/family/Task"
    ecf_tryno = "1"
    ecf_pass = "abc123"  # noqa S108
    ecf_rid = None
    ecf_timeout = 20
    return EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, ecf_timeout=ecf_timeout)


@pytest.fixture(scope="module")
def _mockers_for_ecflow(session_mocker):
    session_mocker.patch("experiment.scheduler.scheduler.Client")
    session_mocker.patch("experiment.scheduler.scheduler.State")


class TestScheduler:
    # pylint: disable=no-self-use

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_ecflow_client(self, ecflow_task):
        ecf_host = "localhost"
        ecflow_server = EcflowServer(ecf_host)
        EcflowClient(ecflow_server, ecflow_task)

    @pytest.mark.usefixtures("_mockers_for_ecflow")
    def test_start_suite(self, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        def_file = f"{tmpdir}/{suite_name()}.def"
        ecf_host = "localhost"
        ecflow_server = EcflowServer(ecf_host)
        ecflow_server.start_suite(suite_name(), def_file)
