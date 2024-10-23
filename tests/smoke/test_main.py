#!/usr/bin/env python3
"""Smoke tests."""
import pytest

from surfexp.cli import pysfxexp


def test_pysfxexp(tmp_directory, mock_deode):
    argv = [f"{tmp_directory}/out.toml", "name"]
    pysfxexp(argv=argv)
