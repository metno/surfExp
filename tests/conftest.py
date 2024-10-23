import os
import sys

import pytest


def new_main(argv=None):
    print(argv)
    os.system(f"touch {tmp_directory}/out.toml.tmp.{os.getpid()}.toml")


deode = type(sys)("deode")
deode.__path__ = ["/tmp"]
deode.submodule = type(sys)("__main__")
deode.submodule.main = new_main
sys.modules["deode"] = deode
sys.modules["deode.__main__"] = deode.submodule


@pytest.fixture(scope="module")
def tmp_directory(tmp_path_factory):
    """Return a temp directory valid for this module."""
    return tmp_path_factory.getbasetemp().as_posix()


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
