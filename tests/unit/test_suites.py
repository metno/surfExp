import pytest
from deode.config_parser import ConfigParserDefaults, ParsedConfig
from deode.derived_variables import set_times

from surfexp import PACKAGE_DIRECTORY
from surfexp.cli import pysfxexp
from surfexp.suites.offline import SurfexSuiteDefinition


@pytest.fixture(name="mock_submission")
def fixture_mock_submission(session_mocker):
    session_mocker.patch("deode.submission.TaskSettings")


@pytest.fixture(name="sekf_config")
def fixture_sekf_config(tmp_directory):
    output_file = f"{tmp_directory}/config_sekf.toml"
    with open(f"{tmp_directory}/mods_sekf.toml", mode="w", encoding="utf8") as fhandler:
        fhandler.write("[platform]\n")
        fhandler.write(f'scratch = "{tmp_directory}"\n')
        fhandler.write('unix_group = "suv"\n')

    argv = [
        "-o",
        output_file,
        "--case-name",
        "deode_case_name",
        "--plugin-home",
        f"{PACKAGE_DIRECTORY}/..",
        f"{tmp_directory}/mods_sekf.toml",
        f"{PACKAGE_DIRECTORY}/data/config/domains/DRAMMEN.toml",
        f"{PACKAGE_DIRECTORY}/data/config/configurations/sekf.toml",
    ]
    pysfxexp(argv=argv)

    config = ParsedConfig.from_file(
        output_file, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA
    )
    config = config.copy(update=set_times(config))
    config = config.copy({"suite_control": {"do_prep": False}})
    return config


@pytest.mark.usefixtures("mock_submission", "project_directory")
def test_offline_deode_suite(deode_config):
    SurfexSuiteDefinition(deode_config)


@pytest.mark.usefixtures("mock_submission", "project_directory")
def test_offline_suite(default_config):
    SurfexSuiteDefinition(default_config)


@pytest.mark.usefixtures("mock_submission", "project_directory")
def test_offline_sekf_suite(sekf_config):
    SurfexSuiteDefinition(sekf_config)
