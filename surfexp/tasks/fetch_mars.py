"""Fetch MARS task."""
import math
import os
import shutil

from deode.datetime_utils import as_timedelta
from deode.logs import logger
from pysurfex.run import BatchJob

from surfexp.tasks.tasks import PySurfexBaseTask


class FetchMars(PySurfexBaseTask):
    """Fetch mars data.

    Args:
            config (dict): Actual configuration dict

    """

    def __init__(self, config):
        PySurfexBaseTask.__init__(self, config, name="FetchMars")
        try:
            mode = self.config["task.args.mode"]
        except KeyError:
            mode = "default"

        if mode == "an_forcing":
            self.basetime = self.basetime - self.fcint
        self.gribdir = f"{self.platform.get_system_value('casedir')}/grib/{mode}"
        os.makedirs(self.gribdir, exist_ok=True)
        leadtimes = []
        validtime = self.basetime
        ltime = 0
        while validtime <= (self.basetime + self.fcint):
            leadtimes.append(ltime)
            ltime += 1
            validtime = validtime + as_timedelta("PT3600S")
        self.leadtimes = leadtimes
        self.date = self.basetime.strftime("%Y%m%d")
        self.hour = self.basetime.strftime("%H%M")
        lon0 = self.geo.lonrange[0]
        lon0 = int(self.geo.lonrange[0])
        lon1 = math.ceil(self.geo.lonrange[1])
        lat0 = int(self.geo.latrange[0])
        lat1 = math.ceil(self.geo.latrange[1])
        self.area = f"{lat1}/{lon0}/{lat0}/{lon1}"
        self.mars_config = self.config[f"mars.{mode}.config"]
        self.gribfile = f"{self.mars_config}_{self.date}_{self.hour}.grib1"
        self.grib_file_with_path = f"{self.gribdir}/{self.gribfile}"

    def execute(self):
        """Execute the perturb state task.

        Raises:
            NotImplementedError: _description_
        """
        if not os.path.exists(self.grib_file_with_path):
            self.fetch_mars()
        else:
            logger.warning(
                "The file {} is already fetched, consider to clean",
                self.grib_file_with_path,
            )
        self.split_files()

    def fetch_mars(self):
        """Fetch mars."""
        request_file = "request.mars"
        with open(request_file, mode="w", encoding="utf8") as fhandler:
            fhandler.write("")

        try:
            clas = self.config[f"mars.{self.mars_config}.class"]
        except KeyError:
            clas = "RD"
        expver = self.config[f"mars.{self.mars_config}.expver"]
        grid = self.config[f"mars.{self.mars_config}.grid"]
        req = Request(
            action="retrieve",
            dates=self.date,
            hours=self.hour,
            step=self.leadtimes,
            levtype="sfc",
            param="129/134/144/165/166/167/168/169/175/228",
            expver=expver,
            clas=clas,
            typ="an/fc",
            stream="oper",
            target=self.gribfile,
            grid=grid,
            area=self.area,
        )
        with open(request_file, mode="a", encoding="utf8") as rf:
            req.write_request(rf)

        rte = os.environ.copy()
        os.system(f"cat {request_file}")  # noqa S605
        BatchJob(rte).run(f"mars {request_file}")
        shutil.move(self.gribfile, self.gribdir)

    def split_files(self):
        """Split files."""
        rule_file = f"{self.mars_config}_filter1.rule"
        with open(rule_file, mode="w", encoding="utf8") as fhandler:
            fhandler.write("set timeRangeIndicator = 0;\n")
            fhandler.write(
                f'write "{self.gribdir}/{self.mars_config}_split_'
                + f'{self.basetime.strftime("%Y%m%d%H")}+[step].grib1";\n'
            )
        logger.info("grib_filter {} {}", rule_file, self.grib_file_with_path)
        rte = os.environ.copy()
        BatchJob(rte).run(f"grib_filter {rule_file} {self.grib_file_with_path}")
        rule_file = f"{self.mars_config}_filter2.rule"
        with open(rule_file, mode="w", encoding="utf8") as fhandler:
            fhandler.write(
                'print "found indicatorOfParameter=[indicatorOfParameter] '
                + 'timeRangeIndicator=[timeRangeIndicator] date=[date] step=[step]";\n'
            )
            fhandler.write(
                "if (indicatorOfParameter == 228 || indicatorOfParameter == 144 "
                + "|| indicatorOfParameter == 169 || indicatorOfParameter == 175) {\n"
            )
            fhandler.write("  set timeRangeIndicator = 4;\n")
            fhandler.write("}\n")
            fhandler.write("write;\n")
        for ltime in self.leadtimes:
            infile = (
                f"{self.gribdir}/{self.mars_config}_split_"
                + f"{self.basetime.strftime('%Y%m%d%H')}+{ltime}.grib1"
            )
            outfile = (
                f"{self.gribdir}/{self.mars_config}_"
                + f"{self.basetime.strftime('%Y%m%d%H')}+{ltime:02d}.grib1"
            )
            if os.path.exists(infile):
                BatchJob(rte).run(f"grib_filter -o {outfile} {rule_file} {infile}")
            else:
                raise FileNotFoundError(f"Infile {infile} is missing")


class FetchMarsPrep(PySurfexBaseTask):
    """Fetch mars grib file for PREP.

    Args:
        config (dict): Actual configuration dict
    """

    def __init__(self, config):
        PySurfexBaseTask.__init__(self, config, name="FetchMarsPrep")
        gribfile = self.config["prep.args.prep-file"]
        gribfile = self.platform.substitute(gribfile)
        gribdir = os.path.dirname(gribfile)
        gribdir = f"{self.platform.get_system_value('casedir')}/grib/"
        os.makedirs(gribdir, exist_ok=True)
        self.date = self.basetime.strftime("%Y%m%d")
        self.hour = self.basetime.strftime("%H%M")
        lon0 = self.geo.lonrange[0]
        lon0 = int(self.geo.lonrange[0])
        lon1 = math.ceil(self.geo.lonrange[1])
        lat0 = int(self.geo.latrange[0])
        lat1 = math.ceil(self.geo.latrange[1])
        self.area = f"{lat1}/{lon0}/{lat0}/{lon1}"
        self.mars_config = self.config["mars.prep.config"]
        self.grib_file_with_path = gribfile

    def execute(self):
        """Execute the perturb state task.

        Raises:
            NotImplementedError: _description_
        """
        if not os.path.exists(self.grib_file_with_path):
            self.fetch_mars()
        else:
            logger.warning(
                "The file {} is already fetched, consider to clean",
                self.grib_file_with_path,
            )

    def fetch_mars(self):
        """Fetch mars."""
        request_file = "request.mars"
        with open(request_file, mode="w", encoding="utf8") as fhandler:
            fhandler.write("")

        try:
            clas = self.config[f"mars.{self.mars_config}.class"]
        except KeyError:
            clas = "RD"
        expver = self.config[f"mars.{self.mars_config}.expver"]
        grid = self.config[f"mars.{self.mars_config}.grid"]
        params = "32/33/39/40/41/42/139/141/170/172/183/198/235/236/" + \
                 "35/36/37/38/238/243/244/245/31/34/129"
        req = Request(
            action="retrieve",
            dates=self.date,
            hours=self.hour,
            step=0,
            levtype="sfc",
            param=params,
            expver=expver,
            clas=clas,
            typ="an/fc",
            stream="oper",
            target=f"'{self.grib_file_with_path}'",
            grid=grid,
            area=self.area,
        )
        with open(request_file, mode="a", encoding="utf8") as rf:
            req.write_request(rf)

        rte = os.environ.copy()
        os.system(f"cat {request_file}")  # noqa S605
        BatchJob(rte).run(f"mars {request_file}")


class Request(object):
    """Mars request class."""

    def __init__(
        self,
        action=None,
        source=None,
        dates=None,
        hours=None,
        origin=None,
        typ=None,
        step=None,
        levelist=None,
        param=None,
        levtype=None,
        database=None,
        expver="prod",
        clas="RR",
        stream="oper",
        target=None,
        grid=None,
        area=None,
    ):
        self.action = action
        self.target = target
        self.source = source
        self.database = database
        self.dates = dates if type(dates) is list else [dates]
        self.hours = hours if type(hours) is list else [hours]
        self.origin = origin
        self.type = typ
        self.step = step if type(step) is list else [step]
        self.param = param if type(param) is list else [param]
        self.levelist = levelist if type(levelist) is list else [levelist]
        self.levtype = levtype
        self.expver = expver
        self.marsClass = clas
        self.stream = stream
        self.grid = grid
        self.area = area
        self.expect = (
            len(self.step)
            * len(self.param)
            * len(self.levelist)
            * len(self.dates)
            * len(self.hours)
        )

    def write_request(self, f):
        """Write mars request.

        Args:
            f (str): File name

        """
        separator = "/"
        if self.action == "archive":
            if self.database:
                f.write(
                    "%s,source=%s,database=%s,\n"
                    % (self.action, self.source, self.database)
                )
            else:
                f.write("%s,source=%s,\n" % (self.action, self.source))
        elif self.action == "retrieve":
            f.write(f"{self.action},\n")
        f.write(_line("TARGET", self.target))
        f.write(_line("DATE", separator.join(str(x) for x in self.dates)))
        f.write(_line("TIME", separator.join(str(x) for x in self.hours)))
        if self.origin is not None:
            f.write(_line("ORIGIN", self.origin.upper()))
        f.write(_line("STEP", separator.join(str(x) for x in self.step)))
        if self.levtype.lower() != "sfc".lower():
            f.write(_line("LEVELIST", separator.join(str(x) for x in self.levelist)))
        f.write(_line("PARAM", separator.join(str(x) for x in self.param)))
        f.write(_line("EXPVER", self.expver.lower()))
        f.write(_line("CLASS ", self.marsClass.upper()))
        f.write(_line("LEVTYPE", self.levtype.upper()))
        f.write(_line("TYPE", self.type.upper()))
        f.write(_line("STREAM", self.stream.upper()))
        if self.grid is not None:
            f.write(_line("GRID", self.grid))
        if self.area is not None:
            f.write(_line("AREA", self.area))
        f.write(_line("EXPECT", "ANY", eol=""))


def _line(key, val, eol=","):
    return "    %s= %s%s\n" % (key.ljust(11), val, eol)
