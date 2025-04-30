"""Prefetch task."""
import os
import datetime
import json
import math
import subprocess

from deode.logs import logger

from surfexp.tasks.tasks import PySurfexBaseTask


class PrefetchMars(PySurfexBaseTask):
    """Perturb state task."""

    def __init__(self, config):
        """Construct assim task.

        Args:
            config (dict): Actual configuration dict

        """
        PySurfexBaseTask.__init__(self, config, name="PrefetchMars")

    def execute(self):
        """Execute the perturb state task.

        Raises:
            NotImplementedError: _description_
        """
        dtg = self.dtg
        fcint = self.fcint

        kwargs = {}

        with open(self.wdir + "/domain.json", mode="w", encoding="utf-8") as file_handler:
            json.dump(self.geo.json, file_handler, indent=2)
        kwargs.update({"domain": self.wdir + "/domain.json"})
        
        kwargs.update({"dtg_start": dtg.strftime("%Y%m%d%H")})
        kwargs.update({"dtg_stop": (dtg + fcint).strftime("%Y%m%d%H")})
        dtg0 = dtg - datetime.timedelta(hours=dtg.hour)

        gribdir =  self.platform.get_system_value("casedir") + "/grib/"
        os.makedirs(gribdir, exist_ok=True)
        date = dtg0.strftime("%Y%m%d")
        hour = dtg0.strftime("%H%M")
        print(self.geo.lonrange)
        print(self.geo.latrange)
        lon0 = self.geo.lonrange[0]
        print(self.geo.lonrange[1])
        print(self.geo.latrange[0])
        print(self.geo.latrange[1])
        lon0 = int(self.geo.lonrange[0])
        lon1 = int(math.ceil(self.geo.lonrange[1]))
        lat0 = int(self.geo.latrange[0])
        lat1 = int(math.ceil(self.geo.latrange[1]))
        area = f"{lat1}/{lon0}/{lat0}/{lon1}"
        print(area)
        prefetch(date, hour, gribdir, area, dtg)


class Request(object):

    def __init__(self,
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
                 area=None):
        """ Construct a request for mars"""
        self.action = action
        self.target = target
        self.source = source
        self.database = database
        self.dates = dates if type(dates) == list else [dates]
        self.hours = hours if type(hours) == list else [hours]
        self.origin = origin
        self.type = typ
        self.step = step if type(step) == list else [step]
        self.param = param if type(param) == list else [param]
        self.levelist = levelist if type(levelist) == list else [levelist]
        self.levtype = levtype
        self.expver = expver
        self.marsClass = clas
        self.stream = stream
        self.grid = grid
        self.area = area
        self.expect = len(self.step)*len(self.param)*len(self.levelist)*len(self.dates)*len(self.hours)

    def write_request(self, f):
        separator = '/'
        if self.action == "archive":
            if self.database:
                f.write('%s,source=%s,database=%s,\n' % (self.action,self.source,self.database))
            else:
                f.write('%s,source=%s,\n' % (self.action,self.source))
        elif self.action == "retrieve":
            f.write(f"{self.action},\n")
        f.write(_line('TARGET',self.target))
        f.write(_line('DATE', separator.join(str(x) for x in self.dates)))
        f.write(_line('TIME', separator.join(str(x) for x in self.hours)))
        if self.origin is not None:
            f.write(_line('ORIGIN',self.origin.upper()))
        f.write(_line('STEP',separator.join(str(x) for x in self.step)))
        if self.levtype.lower() != "sfc".lower():
            f.write(_line('LEVELIST',separator.join(str(x) for x in self.levelist)))
        f.write(_line('PARAM',separator.join(str(x) for x in self.param)))
        f.write(_line('EXPVER',self.expver.lower()))
        f.write(_line('CLASS ',self.marsClass.upper()))
        f.write(_line('LEVTYPE',self.levtype.upper()))
        f.write(_line('TYPE',self.type.upper()))
        f.write(_line('STREAM',self.stream.upper()))
        if self.grid is not None:
            f.write(_line('GRID',self.grid))
        if self.area is not None:
            f.write(_line('AREA',self.area))
        f.write(_line('EXPECT',"ANY", eol=""))
        #f.write(_line('EXPECT',self.expect, eol=""))


def _line(key,val,eol=','):
    return "    %s= %s%s\n" % (key.ljust(11),val,eol)


def fetch_mars(date, hour, filedir, outfile, area):

    request_file = "request.mars"
    with open(request_file, 'w') as f:
        f.write("")
    
    leadtimes = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
    grid = "0.04/0.04"
    req = Request(
        action="retrieve",
        dates=date,
        hours=hour,
        step=leadtimes,
        levtype="sfc",
        param="129/134/144/165/166/167/168/169/175/228",
        expver="iekm",
        clas="rd",
        typ="an/fc",
        stream="oper",
        target=outfile,
        grid=grid,
        area=area
    )
    with open(request_file, 'a') as rf:
            req.write_request(rf)
    
    os.system(f"cat {request_file}")
    result = subprocess.run(["mars", request_file])
    result = subprocess.run(["mv",] + [outfile] + [filedir])


def split_files(file_in, dest, basetime):
    rule_file = "dt_filter1.rule"
    with open(rule_file, mode="w", encoding="utf8") as fhandler:
        fhandler.write("set timeRangeIndicator = 0;\n")
        fhandler.write(f'write "{dest}/dt_split_{basetime.strftime("%Y%m%d%H")}+[step].grib1";\n')
    subprocess.run(["grib_filter", rule_file, file_in])
    rule_file = "dt_filter2.rule"
    with open(rule_file, mode="w", encoding="utf8") as fhandler:
        fhandler.write('print "found indicatorOfParameter=[indicatorOfParameter] timeRangeIndicator=[timeRangeIndicator] date=[date] step=[step]";\n')
        fhandler.write('if (indicatorOfParameter == 228 || indicatorOfParameter == 144 || indicatorOfParameter == 169 || indicatorOfParameter == 175) {\n')
        fhandler.write('  set timeRangeIndicator = 4;\n')
        fhandler.write('}\n')
        fhandler.write('write;\n')
    leadtimes = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
    for ltime in leadtimes:
        infile = f"{dest}/dt_split_{basetime.strftime('%Y%m%d%H')}+{ltime}.grib1"
        outfile = f"{dest}/dt_{basetime.strftime('%Y%m%d%H')}+{ltime:02d}.grib1"
        if os.path.exists(infile):
            subprocess.run(["grib_filter", "-o", outfile, rule_file, infile])
        else:
            raise FileNotFoundError(f"Infile {infile} is missing")


def prefetch(date, hour, dest, area, basetime):

    tempfile = f"{date}_{hour}.grib1"
    if not os.path.exists(dest + tempfile):
        fetch_mars(date, hour, dest, tempfile, area)
    else:
        logger.warning("The file {} is already fetched, consider to clean", tempfile)
    split_files(dest + tempfile, dest, basetime)
