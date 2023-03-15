"""Unit testing."""
from pathlib import Path
import logging
from unittest.mock import patch
import pytest


import surfex


from experiment.experiment import ExpFromFiles


@pytest.fixture(scope="module")
def pysurfex_experiment():
    return f"{str(((Path(__file__).parent).parent).parent)}"


@pytest.fixture(scope="module")
def exp_dependencies(pysurfex_experiment, tmp_path_factory):

    tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
    wdir = f"{tmpdir}/test_config"
    exp_name = "test_config"
    host = "unittest"

    pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
    offline_source = f"{tmpdir}/source"

    return ExpFromFiles.setup_files(wdir, exp_name, host, pysurfex,
                                    pysurfex_experiment,
                                    offline_source=offline_source)

@pytest.fixture(scope="module")
def sfx_exp(exp_dependencies):

    stream = None
    with patch('experiment.scheduler.scheduler.ecflow') as mock_ecflow:
        sfx_exp = ExpFromFiles(exp_dependencies, stream=stream)
        sfx_exp.update_setting("COMPILE#TEST_TRUE", True)
        sfx_exp.update_setting("COMPILE#TEST_VALUES", [1, 2, 4])
        sfx_exp.update_setting("COMPILE#TEST_SETTING", "SETTING")
        return sfx_exp


class TestConfig():
    """Test config."""

    def test_check_experiment_path(self, exp_dependencies, pysurfex_experiment):
        """Test if exp_dependencies contain some expected variables."""
        str1 = exp_dependencies["pysurfex_experiment"]
        str2 = pysurfex_experiment
        assert str1 == str2

    def test_read_setting(self, sfx_exp):
        """Read normal settings."""
        logging.debug("Read setting")
        build = sfx_exp.get_setting("COMPILE#TEST_TRUE")
        assert build is True

    @patch('experiment.scheduler.scheduler.ecflow')
    def test_update_setting(self, mock_ecflow, sfx_exp):
        """Update setting."""
        sfx_exp.update_setting("GENERAL#ENSMBR", 2)
        assert sfx_exp.get_setting("GENERAL#ENSMBR") == 2

    def test_dump_json(self, sfx_exp, tmp_path_factory):
        tmpdir = f"{tmp_path_factory.getbasetemp().as_posix()}"
        sfx_exp.dump_json(f"{tmpdir}/dump_json.json", indent=2)

    def test_max_fc_length(self, sfx_exp):
        sfx_exp.max_fc_length()

    def test_setting_is_not(self, sfx_exp):
        assert sfx_exp.setting_is_not("COMPILE#TEST_TRUE", False) is True

    def test_setting_is_not_one_of(self, sfx_exp):
        assert sfx_exp.setting_is_not_one_of("COMPILE#TEST_SETTING", ["NOT_A_SETTING"]) is True

    def test_setting_is_one_of(self, sfx_exp):
        assert sfx_exp.setting_is_one_of("COMPILE#TEST_SETTING", ["SETTING", "NOT_A_SETTING"]) is True

    def test_value_is_not_one_of(self, sfx_exp):
        assert sfx_exp.value_is_not_one_of("COMPILE#TEST_VALUES", 3) is True

    def test_value_is_one_of(self, sfx_exp):
        assert sfx_exp.value_is_one_of("COMPILE#TEST_VALUES", 1) is True

    def test_write_exp_config(self, exp_dependencies):
        ExpFromFiles.write_exp_config(exp_dependencies, configuration="sekf", configuration_file=None)
