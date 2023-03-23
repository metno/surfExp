"""Experiment configuration."""

from .datetime_utils import ProgressFromConfig, as_timedelta
from .logs import get_logger_from_config

NO_DEFAULT_PROVIDED = object()


class Configuration:
    """Configuration object for testing purposes."""

    def __init__(self, config):
        """Construct configuration settings.

        Args:
            config (ParsedConfig): Parsed config

        """
        self.config = config
        self.logger = get_logger_from_config(config)

    def get_setting(self, setting, sep="#", realization=None):
        """Get setting.

        Args:
            setting (str): Setting
            sep (str, optional): _description_. Defaults to "#".
            realization (int, optional): Realization number

        Returns:
            any: Found setting

        """
        items = setting.replace(sep, ".")
        self.logger.info("Could check realization %s", realization)
        return self.config.get_value(items)

    def get_total_unique_cycle_list(self):
        """Get a list of unique start times for the forecasts.

        Returns:
            list: List with time deltas from midnight
        """
        # Create a list of all cycles from all members
        realizations = self.config.get_value("general.realizations")
        if realizations is None or len(realizations) == 0:
            return self.get_cycle_list()
        else:
            cycle_list_all = []
            for realization in realizations:
                cycle_list_all += self.get_cycle_list(realization=realization)

            cycle_list = []
            cycle_list_str = []
            for cycle in cycle_list:
                cycle_str = str(cycle)
                if cycle_str not in cycle_list_str:
                    cycle_list.append(cycle)
                    cycle_list_str.append(str(cycle))
            return cycle_list

    def get_cycle_list(self, realization=None):
        """Get cycle list as time deltas from midnight.

        Args:
            realization (int, optional): Realization number

        Returns:
            list: Cycle list
        """
        cycle_length = as_timedelta(
            self.get_setting("general.times.cycle_length", realization=realization)
        )
        cycle_list = []
        day = as_timedelta("PT24H")

        cycle_time = cycle_length
        while cycle_time <= day:
            cycle_list.append(cycle_time)
            cycle_time += cycle_length
        return cycle_list

    def max_fc_length(self, realization=None):
        """Calculate the max forecast time.

        Args:
            realization (int, optional): Realization number

        Returns:
            NotImplementedError

        """
        return NotImplementedError

    def get_lead_time_list(self, realization=None):
        """Get a list of forecast lead times.

        Args:
            realization (int, optional): Realization number

        Returns:
            NotImplementedError

        """
        return NotImplementedError

    def get_fgint(self, realization=None):
        """Get the fgint.

        Args:
            realization (int, optional): Realization number

        Returns:
            as_timedelta: fgint

        """
        return as_timedelta(
            self.get_setting("general.times.cycle_length", realization=realization)
        )

    def get_fcint(self, realization=None):
        """Get the fcint.

        Args:
            realization (int, optional): Realization number

        Returns:
            as_timedelta:: fcint in seconds

        """
        return as_timedelta(
            self.get_setting("general.times.cycle_length", realization=realization)
        )

    def setting_is(self, setting, value, realization=None):
        """Check if setting is value.

        Args:
            setting (str): Setting
            value (any): Value
            realization (int, optional): Realization number

        Returns:
            bool: True if found, False if not found.
        """
        if self.get_setting(setting, realization=realization) == value:
            return True
        return False

    def setting_is_not(self, setting, value, realization=None):
        """Check if setting is not value.

        Args:
            setting (str): Setting
            value (any): Value
            realization (int, optional): Realization number

        Returns:
            bool: True if not found, False if found.

        """
        found = False
        if self.get_setting(setting, realization=realization) == value:
            found = True

        if found:
            return False
        return True

    def value_is_one_of(self, setting, value, realization=None):
        """Check if the setting contains value.

        Args:
            setting (str): Setting
            value (any): Value
            realization (int, optional): Realization number

        Returns:
            bool: True if found, False if not found.

        """
        found = False
        setting = self.get_setting(setting, realization=realization)
        for test_setting in setting:
            if test_setting == value:
                found = True
        return found

    def value_is_not_one_of(self, setting, value, realization=None):
        """Check if the setting does not contain value.

        Args:
            setting (str): Setting
            value (any): Value
            realization (int, optional): Realization number

        Returns:
            bool: True if not found, False if found.

        """
        found = self.value_is_one_of(setting, value, realization=realization)
        if found:
            return False
        return True

    def setting_is_one_of(self, setting, values, realization=None):
        """Check if setting is one of the provided list of values.

        Args:
            setting (str): Setting
            values (list): Values
            realization (int, optional): Realization number

        Raises:
            TypeError: Values should be a list

        Returns:
            bool: True if found, False if not found.

        """
        found = False
        setting = self.get_setting(setting, realization=realization)
        if not isinstance(values, list):
            raise TypeError("Excpected a list as input, got ", type(values))
        for val in values:
            if setting == val:
                found = True
        return found

    def setting_is_not_one_of(self, setting, values, realization=None):
        """Check if setting is not one of the provided list of values.

        Args:
            setting (str): Setting
            values (list): Values
            realization (int, optional): Realization number

        Returns:
            bool: True if not found, False if found.

        """
        found = self.setting_is_one_of(setting, values, realization=realization)
        if found:
            return False
        return True

    def get_nnco(self, dtg=None, realization=None):
        """Get the active observations.

        Args:
            dtg (as_datetime, optional): Basetime. Defaults to None.
            realization (int, optional): Realization number

        Returns:
            list: List with either 0 or 1

        """
        # Some relevant assimilation settings
        obs_types = self.get_setting("SURFEX.ASSIM.OBS.COBS_M", realization=realization)
        nnco_r = self.get_setting("SURFEX.ASSIM.OBS.NNCO", realization=realization)
        snow_ass = self.get_setting(
            "SURFEX.ASSIM.ISBA.UPDATE_SNOW_CYCLES", realization=realization
        )
        snow_ass_done = False
        progress = ProgressFromConfig(self.config)
        if dtg is None:
            dtg = progress.basetime
        if len(snow_ass) > 0:
            if dtg is not None:
                hhh = int(dtg.strftime("%H"))
                for s_n in snow_ass:
                    if hhh == int(s_n):
                        snow_ass_done = True
        nnco = []
        for ivar, __ in enumerate(obs_types):
            ival = 0
            if nnco_r[ivar] == 1:
                ival = 1
                if obs_types[ivar] == "SWE":
                    if not snow_ass_done:
                        self.logger.info(
                            "Disabling snow assimilation since cycle is not in %s",
                            snow_ass,
                        )
                        ival = 0
            self.logger.debug("ivar=%s ival=%s", ivar, ival)
            nnco.append(ival)

        self.logger.debug("NNCO: %s", nnco)
        return nnco
