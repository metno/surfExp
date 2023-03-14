"""Test setup of an experiment and merging of input."""
# import os
# import sys
import unittest
from pathlib import Path
# import json
# import inspect
# import shutil
import logging
# import toml
# import experiment_scheduler as scheduler
import experiment
import surfex
# import experiment_setup
# import experiment_tasks
# import ecf

TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
ROOT = f"{str((Path(__file__).parent).parent)}"
logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                    level=logging.DEBUG)


class TestSetup(unittest.TestCase):
    """Test config."""

    @classmethod
    def setUpClass(cls):
        pass

    def test_merge(self):
        """Test merge two dicts."""
        dict1 = {
            "key_l1": {
                "key_l2_dict1": "value_l2_dict1",
                "key_l2": {
                    "key_l3": "value_l3",
                    "key_l3_extra": "value_l3_extra",
                }
            }
        }
        dict2 = {
            "key_l1": {
                "key_l2_extra": "value_l2_extra",
                "key_l2": {
                    "key_l3": "value_l3_modified",
                    "key_l3_dict": {
                        "key_l4": "value_l4"
                    }
                }
            }
        }
        dict3 = {
            "key_l1": {
                "key_l2_dict1": "value_l2_dict1",
                "key_l2_extra": "value_l2_extra",
                "key_l2": {
                    "key_l3": "value_l3_modified",
                    "key_l3_extra": "value_l3_extra",
                    "key_l3_dict": {
                        "key_l4": "value_l4"
                    }
                }
            }
        }
        dict_n3 = experiment.ExpFromFiles.merge_dict(dict1, dict2)
        self.assertEqual(dict3, dict_n3, "Dicts differ")
