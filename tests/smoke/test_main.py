#!/usr/bin/env python3
"""Smoke tests."""
import os
import pytest

from surfexp.cli import pysfxexp


def test_pysfxexp(tmp_directory):
    argv = [f"{tmp_directory}/out.toml", f"{os.getcwd()}../", "name"]
    pysfxexp(argv=argv)
