"""Handling of date/time information."""
import os
import logging
import json


from .datetime_utils import datetime_as_string, as_datetime


class Progress():
    """
    Progress.

    For internal use in experiment on HOST0.

    """

    def __init__(self, dtg, dtgbeg, dtgend=None, dtgpp=None, stream=None):
        """Initialize the experiment progress.

        Args:
            dtg (datetime.datetime): current date/time information
            dtgbeg (datetime.datetime): first date/time information
            dtgend (datetime.datetime): last date/time information
            dtgpp (datetime.datetime): current post-processing date/time information

        """
        # Update DTG
        self.dtg = dtg
        if dtg is None:
            raise AttributeError("You must set a basetime")

        # Update DTGEND
        self.dtgend = dtgend
        if dtgend is None:
            raise AttributeError("You must set end time")

        # Update DTGBEG
        self.dtgbeg = dtgbeg
        if dtgbeg is None:
            raise AttributeError("You must set start time")
        self.dtgpp = dtgpp
        if dtgpp is None:
            self.dtgpp = self.dtg

        self.stream = stream
        logging.debug("DTG: %s", self.dtg)
        logging.debug("DTGBEG: %s", self.dtgbeg)
        logging.debug("DTGEND: %s", self.dtgend)
        logging.debug("DTGPP: %s", self.dtgpp)
        logging.debug("STREAM: %s", self.stream)
        logging.debug("Progress file name: %s", self.get_progress_file_name(""))
        logging.debug("Progress PP file name: %s", self.get_progress_pp_file_name(""))

    @staticmethod
    def string2datetime(dtg_string):
        return as_datetime(dtg_string)

    def print_config_times(self):
        """_summary_

        Args:
            config (datetime.datetime, optional): _description_. Defaults to None.
        """
        update = {
            "start": datetime_as_string(self.dtgbeg),
            "end": datetime_as_string(self.dtgend),
            "validtime": datetime_as_string(self.dtg),
            "basetime": datetime_as_string(self.dtg)
        }
        return update

    def save_as_json(self, exp_dir, progress=False, progress_pp=False, indent=None):
        """Save progress to file.

        Args:
            exp_dir (str): Location of progress file
            progress (bool): Progress file
            progress_pp (bool): Post-processing progress file
            indent (int, optional): Indentation in json file. Defaults to None.

        """
        progress_dict = {
            "DTGBEG": datetime_as_string(self.dtgbeg),
            "DTG": datetime_as_string(self.dtg),
            "DTGEND": datetime_as_string(self.dtgend)
        }
        progress_pp_dict = {
            "DTGPP": datetime_as_string(self.dtgpp)
        }

        if progress:
            progress_file = self.get_progress_file_name(exp_dir, self.stream)
            logging.debug("progress file: %s", progress_file)
            with open(progress_file, mode="w", encoding="utf-8") as progress_file:
                json.dump(progress_dict, progress_file, indent=indent)
        if progress_pp:
            progress_pp_file = self.get_progress_pp_file_name(exp_dir, stream=self.stream)
            logging.debug("progress_pp: %s", progress_pp_file)
            with open(progress_pp_file, mode="w", encoding="UTF-8") as progress_pp_file:
                json.dump(progress_pp_dict, progress_pp_file, indent=indent)

    @staticmethod
    def get_progress_file_name(exp_dir, stream=None, suffix="json"):
        """Get the progress file name

        Args:
            exp_dir (str): Experiment directory
            stream (str, optional): Stream number. Defaults to None.
            suffix (str, optional): File suffix. Defaults to "json".

        Returns:
            str: File name

        """
        stream_txt = ""
        if stream is not None:
            stream_txt = f"_stream{stream}_"
        return f"{exp_dir}/progress{stream_txt}.{suffix}"

    @staticmethod
    def get_progress_pp_file_name(exp_dir, stream=None,  suffix="json"):
        """Get the progress PP file name

        Args:
            exp_dir (str): Experiment directory
            stream (str, optional): Stream number. Defaults to None.
            suffix (str, optional): File suffix. Defaults to "json".

        Returns:
            str: File name

        """
        stream_txt = ""
        if stream is not None:
            stream_txt = f"_stream{stream}_"
        return f"{exp_dir}/progress{stream_txt}PP.{suffix}"


class ProgressFromFiles(Progress):
    """Create progress object from a json file."""

    def __init__(self, exp_dir, dtg=None, dtgbeg=None, dtgpp=None, dtgend=None, stream=None):
        """Initialize a progress object from files.

        Args:
            exp_dir (str): Location of progress files
            stream (str, optional): Stream. Defaults to None.

        """
        progress_file = Progress.get_progress_file_name(exp_dir, stream=stream)
        progress_pp_file = Progress.get_progress_pp_file_name(exp_dir, stream=stream)
        if os.path.exists(progress_file):
            with open(progress_file, mode="r", encoding="utf-8") as file_handler:
                progress = json.load(file_handler)
                dtg_file = progress.get("DTG")
                if dtg is None:
                    dtg = as_datetime(dtg_file)
                dtgbeg_file = progress.get("DTGBEG")
                if dtgbeg is None:
                    dtgbeg = as_datetime(dtgbeg_file)
                dtgend_file = progress.get("DTGEND")
                if dtgend is None:
                    dtgend = as_datetime(dtgend_file)
        else:
            raise FileNotFoundError(progress_file)

        if os.path.exists(progress_pp_file):
            with open(progress_pp_file, mode="r", encoding="utf-8") as file_handler:
                dtgpp_file = json.load(file_handler).get("DTGPP")
                if dtgpp is None:
                    dtgpp = as_datetime(dtgpp_file)
        else:
            raise FileNotFoundError(progress_pp_file)

        Progress.__init__(self, dtg, dtgbeg, dtgend=dtgend, dtgpp=dtgpp)


class ProgressFromConfig(Progress):
    """Create progress object from a json file."""

    def __init__(self, config):
        """Initialize a progress object from files.

        Args:
            config (str): Config

        """
        basetime = as_datetime(config.get_value("general.times.basetime"))
        starttime = as_datetime(config.get_value("general.times.start"))
        endtime = as_datetime(config.get_value("general.times.end"))
        basetime_pp = as_datetime(config.get_value("general.times.basetime"))
        Progress.__init__(self, basetime, starttime, dtgend=endtime, dtgpp=basetime_pp)
