import json

import pytest

from surfexp.tasks.tasks import OptimalInterpolation, QualityControl


@pytest.fixture
def _mockers_for_task_run_tests(session_mocker):
    session_mocker.patch("surfexp.tasks.tasks.titan")
    session_mocker.patch("surfexp.tasks.tasks.gridpp")


@pytest.mark.usefixtures("_mockers_for_task_run_tests")
def test_titan_sets(default_config, tmp_directory):
    blacklist_file = f"{tmp_directory}/blacklist.json"
    with open(blacklist_file, mode="w", encoding="utf8") as fhandler:
        json.dump({}, fhandler)
    variables = ["t2m", "rh2m", "sd"]
    for var in variables:
        task_config = default_config
        update = {
            "general": {"case": "test_titan_sets"},
            "platform": {
                "scratch": tmp_directory,
            },
            "task": {"args": {"var_name": var, "offset": "2"}},
            "observations": {
                "synop_obs_t2m": True,
                "synop_obs_rhm": True,
                "synop_obs_sd": True,
                "netatmo_obs_t2m": True,
                "netatmo_obs_rh2m": True,
                "cryo_obs_sd": True,
                "filepattern": "/dev/null",
                "qc": {"blacklist": blacklist_file},
            },
        }
        task_config = task_config.copy(update)
        QualityControl(task_config).execute()


@pytest.mark.usefixtures("_mockers_for_task_run_tests")
def test_gridpp_vars(default_config, tmp_directory):
    variables = ["t2m", "rh2m", "sd"]
    for var in variables:
        task_config = default_config
        update = {
            "general": {"case": "test_gridpp_vars"},
            "platform": {
                "scratch": tmp_directory,
            },
            "task": {"args": {"var_name": var, "offset": "2"}},
        }
        task_config = task_config.copy(update)
        OptimalInterpolation(task_config).execute()
