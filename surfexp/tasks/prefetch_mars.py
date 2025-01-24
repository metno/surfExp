"""Prefetch task."""
import os
import shutil
import datetime
import json
#import logging
import yaml
from netCDF4 import Dataset
import numpy as np
import subprocess
from deode.datetime_utils import as_datetime, as_timedelta
from surfexp.tasks.tasks import PySurfexBaseTask

try:
    import eccodes as ec
except:
    print("eccodes not loaded")


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
        fcint = as_timedelta(self.config["general.times.cycle_length"])
        #dtend = as_datetime(self.config["general.times.end"]) + fcint
        #print(dtend)
        ntimes = int((fcint)/datetime.timedelta(hours=1))
        print(ntimes)
        dts = [dtg0 + datetime.timedelta(hours=i*3) for i in range(ntimes+2)]
        # TODO is it possible to get the dtgend value and prefetch the whole period?
        print(dts)
        gribdir =  self.platform.get_system_value("casedir") + "/grib/"
        os.makedirs(gribdir, exist_ok=True)
        prefetch(dts, gribdir)


class PrefetchMarsObs(PySurfexBaseTask):
    """Perturb state task."""

    def __init__(self, config):
        """Construct assim task.

        Args:
            config (dict): Actual configuration dict

        """
        PySurfexBaseTask.__init__(self, config, name="PrefetchMarsObs")
        
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
        obsdir =  self.platform.substitute(self.config["system.obs_dir"])
        os.makedirs(obsdir, exist_ok=True)
        prefetch_synop(dtg, obsdir)


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
                 target=None):
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
        f.write(_line('EXPECT',"ANY", eol=""))
        #f.write(_line('EXPECT',self.expect, eol=""))


def _line(key,val,eol=','):
    return "    %s= %s%s\n" % (key.ljust(11),val,eol)


def last_cycle(dt):
    if dt.hour in [i*3 for i in range(8)]:
        return dt
    else:
        return last_cycle(dt - datetime.timedelta(hours=1))


def get_basetime(gid):
    date = ec.codes_get(gid, "date")
    hour = ec.codes_get(gid, "hour")
    return datetime.datetime.strptime(str(date), "%Y%m%d") + datetime.timedelta(hours=hour)


def get_validtime(gid):
    step = ec.codes_get(gid, "step")
    return get_basetime(gid) + datetime.timedelta(hours=step) 


def fill_pattern(pattern, values):
    s = str(pattern)
    for key in values:
        s = s.replace(f"@{key}@", values[key])
    return s


def get_info(dt):
    return {"yyyy": dt.strftime("%Y"),
        "mm": dt.strftime("%m"),
        "dd": dt.strftime("%d"),
        "hh": dt.strftime("%H"),
        }


def fetch_mars(dts, filedir, outfile):

    request_file = "request.mars"
    with open(request_file, 'w') as f:
        f.write("")
    
    #outfiles = []
    
    dates = list(set([dt.strftime("%Y%m%d") for dt in dts]))
    hours = [str(i*3) for i in range(8)]
    #outfile = dts[0].strftime("multi_carra_%Y%m%dT%HZ.grib2")
    #outfiles.append(outfile)
    req = Request(
        action="retrieve",
        dates=dates,
        hours=hours,
        origin="no-ar-ce",
        step=[0],
        levtype="sfc",
        param=params_inst + [228002],#dts = [dts[1]]
         expver="prod",
        clas="rr",
        typ="an",
        stream="oper",
        target=outfile
    )
    req_acc = Request(
        action="retrieve",
        dates=dates,
        hours=hours,
        origin="no-ar-ce",
        step=[1, 2, 3],
        levtype="sfc",
        param=params_acc + params_inst,
        expver="prod",
        clas="rr",
        typ="fc",
        stream="oper",
        target=outfile
    )
    req_ml = Request(
        action="retrieve",
        dates=dates,
        hours=hours,
        origin="no-ar-ce",
        step=[0,1,2],
        levtype="ml",
        levelist=[65],
        param=params_ml,
        expver="prod",
        clas="rr",
        typ="an/fc",
        stream="oper",
        target=outfile
    )
    with open(request_file, 'a') as rf:
            req.write_request(rf)
            req_acc.write_request(rf)
            req_ml.write_request(rf)
        
    result = subprocess.run(["mars", request_file])
    result = subprocess.run(["mv",] + [outfile] + [filedir])


def split_files(file_in, dest):
    filepattern = "carra_@yyyy@@mm@@dd@T@hh@Z_@lll@.grib2"
    #file_in = fill_pattern("carra_@yyyy@@mm@@dd@T@hh@Z.grib2", get_info(dt))
    
    with open(file_in, "rb") as fin:
        outputset = {}
        while True:
            gid = ec.codes_grib_new_from_file(fin)
            if gid is None:
                break
            dt = get_basetime(gid)
            info = get_info(dt)

            step = ec.codes_get(gid, "step")
            info["lll"] = "%03d" % step
            filename = fill_pattern(filepattern, info)
            if filename not in outputset:
                outputset[filename] = []
            
            if step == 0:
                param = ec.codes_get(gid, "param")
                if param in params_ml:
                    gid0 = ec.codes_clone(gid)
                    prev_dt = dt - datetime.timedelta(hours=3)
                    ec.codes_set(gid0, "step", 3)
                    ec.codes_set(gid0, "hour", prev_dt.hour)
                    ec.codes_set(gid0, "date", int(prev_dt.strftime("%Y%m%d")))
                    info0 = get_info(prev_dt)
                    info0["lll"] = "003"
                    filename0 = fill_pattern(filepattern, info0)
                    if filename0 not in outputset:
                        outputset[filename0] = []
                    outputset[filename0].append(gid0)
            if step == 1:
                # copy accumulated and set zero
                param = ec.codes_get(gid, "param")
                if param in params_acc:
                    gid0 = ec.codes_clone(gid)
                    ec.codes_set(gid0, "step", 0)
                    values = ec.codes_get_array(gid0, "values")*0
                    ec.codes_set_values(gid0, values)
                    info["lll"] = "%03d" % 0
                    filename0 = fill_pattern(filepattern, info)
                    if filename0 not in outputset:
                        outputset[filename0] = []
                    outputset[filename0].append(gid0)        
            outputset[filename].append(ec.codes_clone(gid))
            ec.codes_release(gid)
    
    for file_out in outputset:
        if os.path.exists(dest + file_out):
            mode = "ab"
        else:
            mode = "wb"
        with open(dest + file_out, mode) as fout:
            for gid in outputset[file_out]:
                ec.codes_write(gid, fout)
                ec.codes_release(gid)


# accumulated parameters
params_acc = [
          228228,  # total precip
          260645,  # solid precip
          169, # short wave
          260264,  # direct short wave
          175,  # long wave
          ]
# instantaneous
params_inst = [#167,  # t2m
          134,  # surface pressure
          #174096,  # specific humidity
          260260,  # wind dir
          207,  # wind speed
          ]
# model level parameters
params_ml = [
    130, # temperature
    133  # q humidity
]

def prefetch(dts, dest):

    #dts = [datetime.datetime(2015, 9, 8) + datetime.timedelta(hours=i*3) for i in range(8)]
    #dest = "/scratch/fab0/tmp/"

    #tempfile = dts[0].strftime("multi_carra_%Y%m%dT%HZ.grib2")
    tempfile = f"multi_carra_{dts[0].strftime('%Y%m%d')}_{dts[-1].strftime('%Y%m%d')}.grib2"
    if not os.path.exists(dest + tempfile):
        fetch_mars(dts, dest, tempfile)
    else:
        print("WARNING! the data is already fetched, consider to clean")
    split_files(dest + tempfile, dest)


def prefetch_synop(dt, dest):
    
    dt_ = dt - datetime.timedelta(hours=1.5)
    date = dt_.strftime("%Y%m%d")
    ea_date = datetime.datetime(2002, 1, 1, tzinfo=datetime.timezone.utc)
    time = dt_.strftime("%H")
    time_ea = dt.strftime("%H")
    target = f'ob{dt.strftime("%Y%m%d%H")}'
    
    if dt < ea_date:
      request = f'''retrieve,
        class   = E4,
        repres  = BUFR,
        type    = AI,
        obsgroup= con,
        obstype = lsd,
        date    = {date},
        time    = {time_ea},
        area    = 90/0/60/45,
        range   = 180,
        target  = {target}'''
    else:
        request = f'''retrieve,
        type    = ob,
        obsgroup= con,
        obstype = lsd,
        date    = {date},
        time    = {time},
        range   = 180,
        area    = 90/0/60/45,
        target  = {target}'''
        
    request_file = "request.out"
    with open(request_file, 'w') as f:
        f.write(request)
    result = subprocess.run(["mars", request_file])
    result = subprocess.run(["mv",] + [target] + [dest])

