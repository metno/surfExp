import os
import sys

import pytest

from deode.config_parser import ParsedConfig, ConfigParserDefaults
from deode.derived_variables import set_times

from surfexp.cli import pysfxexp

def new_main(argv=None):
    print(argv)
    os.system(f"touch {tmp_directory}/out.toml.tmp.{os.getpid()}.toml")

'''
deode = type(sys)("deode")
deode.__path__ = ["/tmp"]
deode.submodule = type(sys)("__main__")
deode.submodule.main = new_main
sys.modules["deode"] = deode
sys.modules["deode.__main__"] = deode.submodule
'''

@pytest.fixture(scope="module")
def tmp_directory(tmp_path_factory):
    """Return a temp directory valid for this module."""
    return tmp_path_factory.getbasetemp().as_posix()



'''
@pytest.fixture(name="mock_deode", scope="module")
def fixture_mock_deode(session_mocker, tmp_directory):
    def new_main(argv=None):
        print(argv)
        os.system(f"touch {tmp_directory}/out.toml.tmp.{os.getpid()}.toml")

    deode = type(sys)("deode")
    deode.submodule = type(sys)("__main__")
    deode.submodule.main = new_main
    sys.modules["deode"] = deode
    sys.modules["deode.__main__"] = deode.submodule

    session_mocker.patch("surfexp.cli.main", new=new_main)
'''

@pytest.fixture(name="mock_submission", scope="module")
def fixture_mock_submission(session_mocker, tmp_directory):
    session_mocker.patch("deode.submission.TaskSettings")

@pytest.fixture()
def deode_config(tmp_directory):
    output_file = f"{tmp_directory}/config.toml"
    with open(f"{tmp_directory}/mods.toml", mode="w") as fhandler:
        fhandler.write("[platform]\n")
        fhandler.write(f'scratch = "{tmp_directory}"\n')

    argv = [output_file, "case_name", f"{os.getcwd()}/", f"{tmp_directory}/mods.toml", f"{os.getcwd()}/surfexp/data/config/domains/DRAMMEN.toml", f"{os.getcwd()}/surfexp/data/config/mods/dt_an_forcing.toml"]
    pysfxexp(argv=argv)

    config = ParsedConfig.from_file(
            output_file, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA)
    config = config.copy(update=set_times(config))
    return config
