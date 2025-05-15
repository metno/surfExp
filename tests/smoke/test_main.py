#!/usr/bin/env python3
"""Smoke tests."""
import os
import pytest

from surfexp.cli import pysfxexp


@pytest.mark.usefixtures("project_directory")
def test_pysfxexp(tmp_directory):
    argv = ["-o", f"{tmp_directory}/out.toml", "--plugin-home", f"{os.getcwd()}../", "--case-name", "name"]
    pysfxexp(argv=argv)
