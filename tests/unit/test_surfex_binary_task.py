import pytest
import os
import contextlib
from pathlib import Path
import yaml

from deode.os_utils import deodemakedirs

from surfexp.tasks.surfex_binary_task import PerturbedRun, OfflinePgd, OfflinePrep, OfflineForecast

@contextlib.contextmanager
def working_directory(path):
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)

@pytest.fixture(name="emulate_offline_binary")
def fixture_emulate_offline_binary(tmp_directory):
    binary = f"{tmp_directory}/OFFLINE"
    with open(binary, mode="w", encoding="utf8") as fhandler:
        fhandler.write("#!/bin/bash\n")
        fhandler.write("touch SURFOUT.nc\n")
        fhandler.write("touch SURFOUT.20250209_03h00.nc\n")
    os.chmod(binary, 0o0755)
    return tmp_directory


@pytest.fixture(name="emulate_pgd_binary")
def fixture_emulate_pgd_binary(tmp_directory):
    binary = f"{tmp_directory}/PGD"
    with open(binary, mode="w", encoding="utf8") as fhandler:
        fhandler.write("#!/bin/bash\n")
        fhandler.write("touch PGD.nc\n")
        fhandler.write("touch LISTING_PGD.txt\n")
    os.chmod(binary, 0o0755)
    return tmp_directory

@pytest.fixture(name="emulate_prep_binary")
def fixture_emulate_prep_binary(tmp_directory):
    binary = f"{tmp_directory}/PREP"
    with open(binary, mode="w", encoding="utf8") as fhandler:
        fhandler.write("#!/bin/bash\n")
        fhandler.write("touch PREP.nc\n")
    os.chmod(binary, 0o0755)
    return tmp_directory

@pytest.fixture(name="emulate_soda_binary")
def fixture_emulate_soda_binary(tmp_directory):
    binary = f"{tmp_directory}/SODA"
    with open(binary, mode="w", encoding="utf8") as fhandler:
        fhandler.write("#!/bin/bash\n")
        fhandler.write("touch SURFOUT.nc\n")
    os.chmod(binary, 0o0755)
    return tmp_directory

def test_perturbed(deode_config, tmp_directory, emulate_offline_binary):

    path = f"{tmp_directory}/test_perturbed"
    deodemakedirs(path)
    with working_directory(path):

        config = deode_config
        update = {
            "task": {
                "args": {"pert": "1"}
            },
            "system": {
                "bindir": emulate_offline_binary
            }
        }
        config = config.copy(update=update)
        task = PerturbedRun(config)

        forcing_dir = f"{tmp_directory}/deode/case_name/forcing/2025020821/"
        deodemakedirs(forcing_dir)
        climdir = f"{tmp_directory}/deode/case_name/climate/DRAMMEN/"
        deodemakedirs(climdir)
        os.system(f"touch {forcing_dir}/FORCING.nc")
        os.system(f"touch {climdir}/PGD_0215.nc")
        deodemakedirs(f"{tmp_directory}/deode/case_name/20250209_0000/")
        os.system(f"touch {tmp_directory}/deode/case_name/20250209_0000/fc_start_sfx")
        task.execute()

def test_offline(deode_config, tmp_directory, emulate_offline_binary):

    path = f"{tmp_directory}/test_forecast"
    deodemakedirs(path)
    with working_directory(path):

        config = deode_config
        update = {
            "system": {
                "bindir": emulate_offline_binary
            }
        }
        config = config.copy(update=update)
        task = OfflineForecast(config)

        forcing_dir = f"{tmp_directory}/deode/case_name/forcing/2025020900/"
        deodemakedirs(forcing_dir)
        climdir = f"{tmp_directory}/deode/case_name/climate/DRAMMEN/"
        deodemakedirs(climdir)
        os.system(f"touch {forcing_dir}/FORCING.nc")
        os.system(f"touch {climdir}/PGD_0215.nc")
        deodemakedirs(f"{tmp_directory}/deode/case_name/20250209_0000/")
        os.system(f"touch {tmp_directory}/deode/case_name/20250209_0000/fc_start_sfx")
        task.execute()

def test_offline_pgd(deode_config, tmp_directory, emulate_pgd_binary):

    path = f"{tmp_directory}/test_offline_pgd"
    deodemakedirs(path)
    with working_directory(path):

        config = deode_config
        update = {
            "task": {
                "args": {"basetime": "2025020900"}
            },
            "platform": {
                "unix_group": "suv",
            },
            "system": {
                "bindir": emulate_pgd_binary
            }
        }
        config = config.copy(update=update)
        task = OfflinePgd(config)
        task.execute()

def test_offline_prep(deode_config, tmp_directory, emulate_prep_binary):

    path = f"{tmp_directory}/test_offline_prep"
    deodemakedirs(path)
    with working_directory(path):

        config = deode_config
        update = {
            "platform": {
                "unix_group": "suv",
            },
            "system": {
                "bindir": emulate_prep_binary
            }
        }
        config = config.copy(update=update)
        task = OfflinePrep(config)
        task.execute()
