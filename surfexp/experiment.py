"""Experiment tools."""

from pysurfex.namelist import NamelistGenerator, NamelistGeneratorAssemble, NamelistGeneratorAssembleFromFiles
from deode.datetime_utils import as_datetime, as_timedelta
from deode.namelist import NamelistGenerator as DeodeNamelistGenerator
from deode.logs import logger


class SettingsFromNamelist():

    def __init__(self, program, nml, assemble=None):

        self.program = program
        if assemble is not None:
            nam_gen = NamelistGeneratorAssemble(self.program, nml, assemble)
        else:
            nam_gen = NamelistGenerator(self.program, nml)
        self.nam_gen = nam_gen
        self.nml = nam_gen.get_namelist()

    def get_setting(self, setting, sep="#", default=None):
        """Get setting.

        Args:
            setting (str): Setting
            sep (str, optional): _description_. Defaults to "#".

        Returns:
            any: Found setting

        """
        indices = setting.split(sep)
        try:
            return self.nml[indices[0]][indices[1]]
        except KeyError:
            if default is not None:
                logger.warning("Namelist setting {} not found. Using default value: {}", setting, default)
                return default
            else:
                raise RuntimeError from KeyError

    def setting_is(self, setting, value, sep="#"):
        """Check if setting is value.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            setting (str): Setting
            value (any): Value
            realization (int, optional): Realization number

        Returns:
            bool: True if found, False if not found.

        """
        if self.get_setting(setting, sep=sep) == value:
            return True
        return False


    def get_nnco(self, config, basetime=None):
        """Get the active observations.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            basetime (as_datetime, optional): Basetime. Defaults to None.

        Returns:
            list: List with either 0 or 1

        """
        if self.program != "soda":
            raise RuntimeError

        # Some relevant assimilation settings
        obs_types = self.get_setting("NAM_OBS#COBS_M")
        nnco_r = self.get_setting("NAM_OBS#NNCO")

        snow_ass = config["assim.update_snow_cycles"]
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



class SettingsFromNamelistAndConfig(SettingsFromNamelist):

    def __init__(self, program, config):
        # SURFEX: Namelists and input data
        nam_defs = "/home/sbu/projects/surfExp/surfexp/data/config/nam/surfex_namelists.yml"
        assemble = "/home/sbu/projects/surfExp/surfexp/data/config/nam/default_asssemble.yml"
        nlgen_surfex = NamelistGeneratorAssembleFromFiles(program, nam_defs, assemble)
        settings = nlgen_surfex.get_namelist()
        SettingsFromNamelist.__init__(self, program, settings, assemble=None)


class SettingsFromNamelistAndConfigDeode(SettingsFromNamelist):

    def __init__(self, program, config):
        # SURFEX: Namelists and input data
        nlgen_surfex = DeodeNamelistGenerator(config, "surfex")
        nlgen_surfex.load(program)
        settings = nlgen_surfex.assemble_namelist(program)
        SettingsFromNamelist.__init__(self, program, settings, assemble=None)


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


def get_cycle_list(config):
    """Get cycle list as time deltas from midnight.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.

    Returns:
        list: Cycle list

    """
    cycle_length = get_fgint(config)
    cycle_list = []
    day = as_timedelta("PT24H")

    cycle_time = cycle_length
    while cycle_time <= day:
        cycle_list.append(cycle_time)
        cycle_time += cycle_length
    return cycle_list


def get_fgint(config):
    """Get the fgint.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.

    Returns:
        as_timedelta: fgint

    """
    return as_timedelta(config["general.times.cycle_length"])
