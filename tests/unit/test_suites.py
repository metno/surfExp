import pytest

from surfexp.suites.offline import SurfexSuiteDefinition


def test_offline_suite(deode_config, mock_submission):

    suite = SurfexSuiteDefinition(deode_config)
