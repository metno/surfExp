"""Unit testing."""
import logging
from pathlib import Path

import pytest
import surfex

from experiment.configuration import Configuration
from experiment.experiment import ExpFromFiles


@pytest.fixture(scope="module")
def pysurfex_experiment():
    return f"{str(((Path(__file__).parent).parent).parent)}"


@pytest.fixture(scope="module")
def exp_dependencies(pysurfex_experiment, tmp_path_factory):
    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    wdir = f"{tmpdir}/test_config"
    exp_name = "test_config"
    host = "ECMWF-atos"

    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = f"{tmpdir}/source"

    return ExpFromFiles.setup_files(
        wdir, exp_name, host, pysurfex, pysurfex_experiment, offline_source=offline_source
    )


@pytest.fixture(scope="module")
def settings(sfx_exp):
    return Configuration(sfx_exp.config)


@pytest.fixture(scope="module")
def sfx_exp(exp_dependencies):
    stream = None
    sfx_exp = ExpFromFiles(exp_dependencies, stream=stream)
    update = {
        "compile": {
            "test_true": True,
            "test_values": [1, 2, 4],
            "test_setting": "SETTING",
        }
    }
    sfx_exp.config = sfx_exp.config.copy(update=update)
    return sfx_exp


@pytest.fixture(scope="module")
def _mockers_for_ecflow(session_mocker):
    session_mocker.patch("experiment.scheduler.scheduler.ecflow.Client")
    session_mocker.patch("experiment.scheduler.submission.TaskSettings.parse_job")
    session_mocker.patch("experiment.scheduler.suites.ecflow.Defs")


class TestConfig:
    """Test config."""

    def test_check_experiment_path(self, exp_dependencies, pysurfex_experiment):
        """Test if exp_dependencies contain some expected variables."""
        str1 = exp_dependencies["pysurfex_experiment"]
        str2 = pysurfex_experiment
        assert str1 == str2

    def test_read_setting(self, settings):
        """Read normal settings."""
        logging.debug("Read setting")
        build = settings.get_setting("compile#test_true")
        assert build is True

    def test_dump_json(self, sfx_exp, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        sfx_exp.dump_json(f"{tmpdir}/dump_json.json", indent=2)

    def test_setting_is_not(self, settings):
        assert settings.setting_is_not("compile#test_true", False) is True

    def test_setting_is_not_one_of(self, settings):
        assert (
            settings.setting_is_not_one_of("compile#test_setting", ["NOT_A_SETTING"])
            is True
        )

    def test_setting_is_one_of(self, settings):
        assert (
            settings.setting_is_one_of(
                "compile#test_setting", ["SETTING", "NOT_A_SETTING"]
            )
            is True
        )

    def test_value_is_not_one_of(self, settings):
        assert settings.value_is_not_one_of("compile#test_values", 3) is True

    def test_value_is_one_of(self, settings):
        assert settings.value_is_one_of("compile#test_values", 1) is True

    def test_write_exp_config(self, exp_dependencies):
        ExpFromFiles.write_exp_config(
            exp_dependencies, configuration="sekf", configuration_file=None
        )
