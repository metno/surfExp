"""Handling of date/time information."""
import os
from datetime import datetime
import logging
import json


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

        # Update DTGEND
        self.dtgend = dtgend

        # Update DTGBEG
        self.dtgbeg = dtgbeg

        self.dtgpp = dtgpp
        if dtgpp is None:
            self.dtgpp = self.dtg

        self.stream = stream
        logging.debug("DTG: %s", self.dtg_string)
        logging.debug("DTGBEG: %s", self.dtgbeg_string)
        logging.debug("DTGEND: %s", self.dtgend_string)
        logging.debug("DTGPP: %s", self.dtgpp_string)
        logging.debug("STREAM: %s", self.stream)
        logging.debug("Progress file name: %s", self.get_progress_file_name(""))
        logging.debug("Progress PP file name: %s", self.get_progress_pp_file_name(""))

    @staticmethod
    def string2datetime(dtg_string):
        if isinstance(dtg_string, str):
            if len(dtg_string) == 10:
                dtg = datetime.strptime(dtg_string, "%Y%m%d%H")
            elif len(dtg_string) == 12:
                dtg = datetime.strptime(dtg_string, "%Y%m%d%H%M")
            else:
                raise Exception("DTG strings must be YYYYMMDDHH or YYYYMMDDHHmm")
        elif isinstance(dtg_string, datetime):
            dtg = dtg_string
        else:
            raise Exception("Unknown DTG input")
        return dtg

    def __getattr__(self, name):
        if name[-7:] == "_string":
            base = name[0:-7]
            if base in self.__dict__:
                val = self.__dict__[base]
                if isinstance(val, datetime):
                    return val.strftime("%Y%m%d%H%M")
                else:
                    raise AttributeError
            else:
                raise AttributeError
        else:
            raise AttributeError

    def update(self, dtg=None, dtgpp=None):
        """_summary_

        Args:
            dtg (datetime.datetime, optional): _description_. Defaults to None.
            dtgpp (datetime.datetime, optional): _description_. Defaults to None.
        """

        if dtg is not None:
            dtg = self.string2datetime(dtg)
            # Update DTG
            self.dtg = dtg

        if dtgpp is not None:
            dtgpp = self.string2datetime(dtgpp)
            self.dtgpp = dtgpp

    def save_as_json(self, exp_dir, progress=False, progress_pp=False, indent=None):
        """Save progress to file.

        Args:
            exp_dir (str): Location of progress file
            progress (bool): Progress file
            progress_pp (bool): Post-processing progress file
            indent (int, optional): Indentation in json file. Defaults to None.

        """
        progress_dict = {
            "DTGBEG": self.dtgbeg_string,
            "DTG": self.dtg_string,
            "DTGEND": self.dtgend_string
        }
        progress_pp_dict = {
            "DTGPP": self.dtgpp_string,
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


class ProgressFromDict(Progress):

    """Create progress object from a dict."""

    def __init__(self, progress):
        """Initialize a progress object from a dict.

        Args:
            progress_file (dict): Progress dict

        """
        dtg = progress.get("DTG")
        if dtg is not None:
            dtg = datetime.strptime(dtg, "%Y%m%d%H%M")
        dtgbeg = progress.get("DTGBEG")
        if dtgbeg is not None:
            dtgbeg = datetime.strptime(dtgbeg, "%Y%m%d%H%M")
        dtgend = progress.get("DTGEND")
        if dtgend is not None:
            dtgend = datetime.strptime(dtgend, "%Y%m%d%H%M")
        dtgpp = progress.get("DTGPP")
        if dtgpp is not None:
            dtgpp = datetime.strptime(dtgpp, "%Y%m%d%H%M")
        Progress.__init__(self, dtg, dtgbeg, dtgend=dtgend, dtgpp=dtgpp)


class ProgressFromFiles(Progress):
    """Create progress object from a json file."""

    def __init__(self, exp_dir, stream=None):
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
                dtg = progress.get("DTG")
                if dtg is not None:
                    dtg = datetime.strptime(dtg, "%Y%m%d%H%M")
                dtgbeg = progress.get("DTGBEG")
                if dtgbeg is not None:
                    dtgbeg = datetime.strptime(dtgbeg, "%Y%m%d%H%M")
                dtgend = progress.get("DTGEND")
                if dtgend is not None:
                    dtgend = datetime.strptime(dtgend, "%Y%m%d%H%M")
        else:
            dtg = None
            dtgbeg = None
            dtgend = None
        dtgpp = None
        if os.path.exists(progress_pp_file):
            with open(progress_pp_file, mode="r", encoding="utf-8") as file_handler:
                dtgpp = json.load(file_handler).get("DTGPP")
                if dtgpp is not None:
                    dtgpp = datetime.strptime(dtgpp, "%Y%m%d%H%M")
        else:
            dtgpp = None

        Progress.__init__(self, dtg, dtgbeg, dtgend=dtgend, dtgpp=dtgpp)
