"""Experiment tools."""
import os
import shutil

import pysurfex
from deode.datetime_utils import as_datetime, as_timedelta
from deode.experiment import ExpFromFiles
from deode.host_actions import DeodeHost
from deode.logs import logger


def get_nnco(config, basetime=None, realization=None):
    """Get the active observations.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        basetime (as_datetime, optional): Basetime. Defaults to None.
        realization (int, optional): Realization number

    Returns:
        list: List with either 0 or 1

    """
    # Some relevant assimilation settings
    obs_types = get_setting(config, "SURFEX.ASSIM.OBS.COBS_M", realization=realization)
    nnco_r = get_setting(config, "SURFEX.ASSIM.OBS.NNCO", realization=realization)
    snow_ass = get_setting(
        config, "SURFEX.ASSIM.ISBA.UPDATE_SNOW_CYCLES", realization=realization
    )
    snow_ass_done = False

    if basetime is None:
        basetime = as_datetime(config["general.times.basetime"])
    if len(snow_ass) > 0 and basetime is not None:
        hhh = int(basetime.strftime("%H"))
        for s_n in snow_ass:
            if hhh == int(s_n):
                snow_ass_done = True
    nnco = []
    for ivar, __ in enumerate(obs_types):
        ival = 0
        if nnco_r[ivar] == 1:
            ival = 1
            if obs_types[ivar] == "SWE" and not snow_ass_done:
                logger.info(
                    "Disabling snow assimilation since cycle is not in {}",
                    snow_ass,
                )
                ival = 0
        logger.debug("ivar={} ival={}", ivar, ival)
        nnco.append(ival)

    logger.debug("NNCO: {}", nnco)
    return nnco


def get_total_unique_cycle_list(config):
    """Get a list of unique start times for the forecasts.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.

    Returns:
        list: List with time deltas from midnight

    """
    # Create a list of all cycles from all members
    realizations = config["general.realizations"]
    if realizations is None or len(realizations) == 0:
        return get_cycle_list(config)

    cycle_list_all = []
    for realization in realizations:
        cycle_list_all += get_cycle_list(config, realization=realization)

    cycle_list = []
    cycle_list_str = []
    for cycle in cycle_list_all:
        cycle_str = str(cycle)
        if cycle_str not in cycle_list_str:
            cycle_list.append(cycle)
            cycle_list_str.append(str(cycle))
    return cycle_list


def get_cycle_list(config, realization=None):
    """Get cycle list as time deltas from midnight.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        realization (int, optional): Realization number

    Returns:
        list: Cycle list

    """
    cycle_length = as_timedelta(
        get_setting(config, "general.times.cycle_length", realization=realization)
    )
    cycle_list = []
    day = as_timedelta("PT24H")

    cycle_time = cycle_length
    while cycle_time <= day:
        cycle_list.append(cycle_time)
        cycle_time += cycle_length
    return cycle_list


def get_setting(config, setting, sep="#", realization=None):
    """Get setting.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        setting (str): Setting
        sep (str, optional): _description_. Defaults to "#".
        realization (int, optional): Realization number

    Returns:
        any: Found setting

    """
    items = setting.replace(sep, ".")
    logger.info("Could check realization {}", realization)
    return config[items]


def setting_is(config, setting, value, realization=None):
    """Check if setting is value.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        setting (str): Setting
        value (any): Value
        realization (int, optional): Realization number

    Returns:
        bool: True if found, False if not found.

    """
    if get_setting(config, setting, realization=realization) == value:
        return True
    return False


def get_fgint(config, realization=None):
    """Get the fgint.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        realization (int, optional): Realization number

    Returns:
        as_timedelta: fgint

    """
    return as_timedelta(
        get_setting(config, "general.times.cycle_length", realization=realization)
    )
