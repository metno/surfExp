"""Test setup of an experiment and merging of input."""
import logging
from pathlib import Path

from experiment.experiment import ExpFromFiles

TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
ROOT = f"{str((Path(__file__).parent).parent)}"
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s",
    level=logging.DEBUG,
)


def test_merge():
    """Test merge two dicts."""
    dict1 = {
        "key_l1": {
            "key_l2_dict1": "value_l2_dict1",
            "key_l2": {
                "key_l3": "value_l3",
                "key_l3_extra": "value_l3_extra",
            },
        }
    }
    dict2 = {
        "key_l1": {
            "key_l2_extra": "value_l2_extra",
            "key_l2": {
                "key_l3": "value_l3_modified",
                "key_l3_dict": {"key_l4": "value_l4"},
            },
        }
    }
    dict3 = {
        "key_l1": {
            "key_l2_dict1": "value_l2_dict1",
            "key_l2_extra": "value_l2_extra",
            "key_l2": {
                "key_l3": "value_l3_modified",
                "key_l3_extra": "value_l3_extra",
                "key_l3_dict": {"key_l4": "value_l4"},
            },
        }
    }
    dict_n3 = ExpFromFiles.merge_dict(dict1, dict2)
    assert dict3 == dict_n3
