import os
import contextlib
from pathlib import Path
import pytest
import types
import sys

from deode.os_utils import deodemakedirs
from deode.tasks.base import Task
from deode.tasks.discover_task import get_task, discover

from surfexp import PACKAGE_DIRECTORY

def available_tasks():
    """Create a list of available tasks.

    Args:
        reg (DeodePluginRegistry): Deode plugin registry

    Returns:
        known_types (list): Task objects

    """
    known_types = {}
    plg_path = f"{PACKAGE_DIRECTORY}/.."
    plg_tasks_path = f"{PACKAGE_DIRECTORY}/tasks"
    plg_name = "surfexp"

    tasks = types.ModuleType(plg_name)
    tasks.__path__ = [plg_tasks_path]
    sys.path.insert(0, plg_path)
    found_types = discover(tasks, Task)
    for ftype, cls in found_types.items():
        if ftype in known_types:
            print("Overriding suite {}", ftype)
        if ftype not in ["pysurfexbase", "surfexbinary"]:
            known_types[ftype] = cls

    return known_types


def classes_to_be_tested():
    """Return the names of the task-related classes to be tested."""
    encountered_classes = available_tasks()
    return encountered_classes.keys()


@contextlib.contextmanager
def working_directory(path):
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)

def create_binaries(casedir, task_name, task_config):

    bindir = f"{casedir}/bin"
    os.makedirs(bindir, exist_ok=True)
    update = {
        "system": {
            "bindir": bindir
        }
    }
    need_pgd = False
    need_prep = False
    task_config = task_config.copy(update)
    if task_name == "offlineforecast" or task_name == "perturbedrun":
        need_pgd = True
        need_prep = True
        if task_name == "perturbedrun":
            forcing_dir = f"{casedir}/forcing/2025020821/"
            diag_output = "SURFOUT.20250209_00h00.nc"
            args = {
                "task": {
                    "args": {
                        "pert": "1"
                    }
                }
            }
            task_config = task_config.copy(args)
            archive = f"{casedir}/archive/2025/02/08/21/"
            deodemakedirs(archive)
            os.system(f"touch {archive}/ANALYSIS.nc")
        else:
            forcing_dir = f"{casedir}/forcing/2025020900/"
            diag_output = "SURFOUT.20250209_03h00.nc"
            archive = f"{casedir}/archive/2025/02/09/00/"
            deodemakedirs(archive)
            os.system(f"touch {archive}/PREP.nc")
        binary = f"{bindir}/OFFLINE"
        with open(binary, mode="w", encoding="utf8") as fhandler:
            fhandler.write("#!/bin/bash\n")
            fhandler.write("touch SURFOUT.nc\n")
            fhandler.write(f"touch {diag_output}\n")
        os.chmod(binary, 0o0755)
        deodemakedirs(forcing_dir)
        os.system(f"touch {forcing_dir}/FORCING.nc")
    elif task_name.lower() == "offlinepgd":

        binary = f"{bindir}/PGD"
        with open(binary, mode="w", encoding="utf8") as fhandler:
            fhandler.write("#!/bin/bash\n")
            fhandler.write("touch PGD.nc\n")
            fhandler.write("touch LISTING_PGD.txt\n")
        os.chmod(binary, 0o0755)
        args = {
            "task": {
                "args": {
                    "basetime": "2025-02-09T00:00:00Z"
                }
            }
        }
        task_config = task_config.copy(args)
    elif task_name.lower() == "offlineprep":
        need_pgd = True
        binary = f"{bindir}/PREP"
        with open(binary, mode="w", encoding="utf8") as fhandler:
            fhandler.write("#!/bin/bash\n")
            fhandler.write("touch PREP.nc\n")
        os.chmod(binary, 0o0755)
        update = {
            "prep": {
                "tolerate_missing": True
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "soda":
        need_pgd = True
        need_prep = True
        binary = f"{bindir}/SODA"
        with open(binary, mode="w", encoding="utf8") as fhandler:
            fhandler.write("#!/bin/bash\n")
            fhandler.write("touch SURFOUT.nc\n")
        os.chmod(binary, 0o0755)

        obdir = f"{casedir}/archive/observations/2025/02/09/00/"
        deodemakedirs(obdir)
        fg_dir = f"{casedir}/archive/2025/02/08/21/"
        deodemakedirs(fg_dir)
        archive = f"{casedir}/archive/2025/02/09/00/"
        deodemakedirs(archive)
        deodemakedirs(f"{casedir}/20250209_0000/")
        os.system(f"touch {fg_dir}/SURFOUT.nc")
        os.system(f"touch {obdir}/OBSERVATIONS_250209H00.DAT")
        os.system(f"touch {archive}/SURFOUT_PERT0.nc")
        os.system(f"touch {archive}/SURFOUT_PERT2.nc")
        os.system(f"touch {archive}/SURFOUT_PERT4.nc")
        os.system(f"touch {archive}/SURFOUT_PERT6.nc")
        os.system(f"touch {archive}/SURFOUT_PERT8.nc")
    else:
        raise NotImplementedError
    if need_pgd:
        climdir = f"{casedir}/climate/DRAMMEN/"
        deodemakedirs(climdir)
        os.system(f"touch {climdir}/PGD_0215.nc")
    if need_prep:
        deodemakedirs(f"{casedir}/20250209_0000/")
        os.system(f"touch {casedir}/20250209_0000/fc_start_sfx")
    return task_config

@pytest.fixture(name="task_name_and_configs", params=classes_to_be_tested(), scope="function")
def fixture_task_name_and_configs(request, default_config, tmp_directory):
    """Return a ParsedConfig with a task-specific section according to `params`."""
    task_name = request.param
    task_config = default_config

    casedir = f"{tmp_directory}/deode/{task_name}"
    update = {
        "general": {
            "case": task_name
        },
        "platform": {
            "scratch": tmp_directory,
        }
    }
    task_config = task_config.copy(update)


    if task_name.lower() in ["offlinepgd", "offlineprep", "offlineforecast", "perturbedrun", "soda"]:
        task_config = create_binaries(casedir, task_name, task_config)
        if task_name.lower() in ["offlineforecast"]:
            update = {
                "task": {
                    "args": {
                        "mode": "forecast"
                    }
                }
            }
            task_config = task_config.copy(update)
    elif task_name.lower() == "qualitycontrol" or task_name.lower() == "optimalinterpolation":
        update = {
            "task": {
                "args": {
                    "var_name": "t2m",
                    "offset": "2"
                }
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "startofflinesfx":
        update = {
            "task": {
                "args": {
                    "run_cmd": "echo 'Hello world!'"
                }
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "harpsqlite":
        update = {
            "task": {
                "args": {
                    "mode": "cycle",
                    "var_name": "T2M",
                    "basetime": "2025-02-09T00:00:00Z",
                    "validtime": "2025-02-09T00:00:00Z"
                }
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "modifyforcing":
        update = {
            "task": {
                "args": {
                    "var_name": "SWdir",
                    "mode": "default"
                }
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "cmakebuild":
        builddir = f"{casedir}/offline/build/bin/"
        deodemakedirs(builddir)
        programs = ["PGD-offline", "PREP-offline", "OFFLINE-offline", "SODA-offline"]
        for program in programs:
            os.system(f"touch {builddir}/{program}")
    elif task_name.lower() == "soil":
        soilgrid_data_path = f"{casedir}/SOILGRID"
        deodemakedirs(soilgrid_data_path)
        update = {
            "platform": {
                "soilgrid_data_path": soilgrid_data_path
            }
        }
        task_config = task_config.copy(update)
        os.system(f"touch {soilgrid_data_path}/CLYPPT.tif")
        os.system(f"touch {soilgrid_data_path}/SNDPPT.tif")
        os.system(f"touch {soilgrid_data_path}/SOC_TOP.tif")
        os.system(f"touch {soilgrid_data_path}/SOC_SUB")
    elif task_name.lower() == "gmted":
        gmted2010_data_path = f"{casedir}/GMTED"
        deodemakedirs(gmted2010_data_path)
        update = {
            "platform": {
                "gmted2010_data_path": gmted2010_data_path
            }
        }
        task_config = task_config.copy(update)
        os.system(f"touch {gmted2010_data_path}/50N000E_20101117_gmted_mea075.tif")
    elif task_name.lower() == "fetchmars":
        deodemakedirs(f"{casedir}/grib/default")
        os.system(f"touch {casedir}/grib/default/sfx_hres_20250209_0000.grib1")
        for t in range(0, 25):
            os.system(f"touch {casedir}/grib/default/sfx_hres_split_2025020900+{t}.grib1")
    elif task_name.lower() == "firstguess4oi":
        archive = f"{casedir}/archive/2025/02/09/00/"
        deodemakedirs(archive)
        os.system(f"touch {archive}/raw.nc")
        update = {
            "task": {
                "args": {
                    "mode": "analysis"
                }
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "qc2obsmon":
        update = {
            "task": {
                "args": {
                    "mode": "an_forcing"
                }
            }
        }
        task_config = task_config.copy(update)
    elif task_name.lower() == "harpsqlite2":
        update = {
            "task": {
                "args": {
                    "basetime": "2025-02-09T00:00:00Z"
                }
            }
        }
        task_config = task_config.copy(update)
    return task_name, task_config


@pytest.fixture(scope="function")
def _mockers_for_task_run_tests(session_mocker):

    session_mocker.patch("surfexp.tasks.compilation.BatchJob")
    session_mocker.patch("surfexp.tasks.tasks.converter2harp_cli")
    session_mocker.patch("surfexp.tasks.tasks.BatchJob")
    session_mocker.patch("surfexp.tasks.fetch_mars.BatchJob")
    session_mocker.patch("surfexp.tasks.tasks.cli_oi2soda")
    session_mocker.patch("surfexp.tasks.tasks.first_guess_for_oi")
    session_mocker.patch("surfexp.tasks.forcing.converter2ds")
    session_mocker.patch("surfexp.tasks.forcing.concat_datasets")
    session_mocker.patch("surfexp.tasks.tasks.titan")
    session_mocker.patch("surfexp.tasks.tasks.gridpp")
    session_mocker.patch("surfexp.tasks.tasks.qc2obsmon")
    session_mocker.patch("surfexp.tasks.surfex_binary_task.pgd")
    session_mocker.patch("surfexp.tasks.surfex_binary_task.prep")
    session_mocker.patch("surfexp.tasks.surfex_binary_task.offline")
    session_mocker.patch("surfexp.tasks.surfex_binary_task.perturbed_offline")
    session_mocker.patch("surfexp.tasks.surfex_binary_task.soda")
    session_mocker.patch("surfexp.tasks.forcing.create_forcing")
    session_mocker.patch("surfexp.tasks.tasks.cryoclim_pseudoobs")
    session_mocker.patch("surfexp.tasks.gmtedsoil.gdal")

class TestTasks:
    """Test all tasks."""

    @pytest.mark.usefixtures("_mockers_for_task_run_tests", "project_directory")
    def test_task_can_be_instantiated(self, task_name_and_configs):
        class_name, task_config = task_name_and_configs
        assert isinstance(get_task(class_name, task_config), Task)

    @pytest.mark.usefixtures("_mockers_for_task_run_tests", "project_directory")
    def test_task_can_be_run(self, task_name_and_configs):
        class_name, task_config = task_name_and_configs
        my_task_class = get_task(class_name, task_config)
        org_cwd = Path.cwd()
        my_task_class.run()
        os.chdir(org_cwd)
