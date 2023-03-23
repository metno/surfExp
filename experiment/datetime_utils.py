#!/usr/bin/env python3
"""Implement helper routines to deal with dates and times."""
from datetime import datetime, timezone

import dateutil.parser
import pandas as pd
from dateutil.utils import default_tzinfo

# The regex in a json schema's "pattern" must use JavaScript syntax (ECMA 262).
# <https://json-schema.org/understanding-json-schema/reference/regular_expressions.html>
ISO_8601_TIME_DURATION_REGEX = "^P(?!$)(\\d+Y)?(\\d+M)?(\\d+W)?(\\d+D)?"
ISO_8601_TIME_DURATION_REGEX += "(T(?=\\d+[HMS])(\\d+H)?(\\d+M)?(\\d+S)?)?$"


def as_datetime(obj):
    """Convert obj to string, parse into datetime and add UTC timezone iff naive."""
    return default_tzinfo(dateutil.parser.parse(str(obj)), tzinfo=timezone.utc)


def as_timedelta(obj):
    """Convert obj to string and parse into pd.Timedelta."""
    return pd.Timedelta(str(obj))


def datetime_as_string(obj):
    """Convert datetime obejct to string."""
    return obj.isoformat(sep="T").replace("+00:00", "Z")


def ecflow2datetime_string(obj):
    """Convert ecflow date string to ISO string."""
    return datetime_as_string(datetime.strptime(obj, "%Y%m%d%H%M")) + "Z"


def datetime2ecflow(obj):
    """Convert ISO datetime to EcFlow string."""
    return obj.strftime("%Y%m%d%H%M")


class ProgressFromConfig:
    """Create progress object from a json file."""

    def __init__(self, config):
        """Initialize a progress object from files.

        Args:
            config (str): Config

        """
        self.basetime = as_datetime(config.get_value("general.times.basetime"))
        self.starttime = as_datetime(config.get_value("general.times.start"))
        self.endtime = as_datetime(config.get_value("general.times.end"))
        self.basetime_pp = as_datetime(config.get_value("general.times.basetime"))
