"""Offline suite."""
import contextlib
from pathlib import Path

from deode.datetime_utils import as_datetime, as_timedelta, get_decadal_list, get_decade
from deode.logs import logger
from deode.suites.base import (
    EcflowSuiteFamily,
    EcflowSuiteTask,
    EcflowSuiteTrigger,
    EcflowSuiteTriggers,
    SuiteDefinition,
)

# TODO should be moved to deode.suites or a module
from ecflow import Limit

from surfexp.experiment import SettingsFromNamelistAndConfig, get_total_unique_cycle_list


class SurfexSuiteDefinition(SuiteDefinition):
    """Surfex suite."""

    def __init__(
        self,
        config,
        dry_run=False,
    ):
        """Initialize a SurfexSuite object.

        Args:
        ----
            suite_name (str): Name of the suite
            config (ParsedConfig): Parsed configuration
            dry_run (str, optional): Dry run. Defaults to False

        Raises:
        ------
            NotImplementedError: Not implmented

        """
        SuiteDefinition.__init__(self, config, dry_run=dry_run)

        template = Path(__file__).parent.resolve() / "../templates/ecflow/default.py"
        template = template.as_posix()

        self.one_decade = config["pgd.one_decade"]
        self.has_mars = False
        self.mode = config["suite_control.mode"]
        self.do_soil = config["suite_control.do_soil"]
        self.do_pgd = config["suite_control.do_pgd"]
        self.do_prep = config["suite_control.do_prep"]
        if self.mode == "restart":
            self.do_prep = False

        input_cycles_ahead = 3
        unique_cycles = get_total_unique_cycle_list(config)
        basetime = as_datetime(config["general.times.basetime"])
        starttime = as_datetime(config["general.times.start"])
        endtime = as_datetime(config["general.times.end"])
        cycle_length = as_timedelta(config["general.times.cycle_length"])
        max_tasks = config.get("general.max_tasks")
        if max_tasks is None:
            max_tasks = 20
        logger.debug("DTGSTART: {} DTGBEG: {} DTGEND: {}", basetime, starttime, endtime)

        limit = Limit("max_tasks", max_tasks)
        self.suite.ecf_node.add_limit(limit)
        self.suite.ecf_node.add_inlimit("max_tasks")

        l_basetime = basetime
        logger.debug("Building list of DTGs")

        cycles = {}
        time_trigger_times = {}
        prediction_trigger_times = {}

        logger.info("unique_cycles={}", unique_cycles)
        cont = True
        while cont:
            for cycle in unique_cycles:
                while l_basetime <= endtime:
                    c_index = l_basetime.strftime("%Y%m%d%H%M")
                    time_fam_start = l_basetime - cycle_length * input_cycles_ahead
                    if time_fam_start >= starttime:
                        time_trigger_times.update(
                            {c_index: time_fam_start.strftime("%Y%m%d%H%M")}
                        )
                    else:
                        time_trigger_times.update({c_index: None})

                    prediction_time = l_basetime - cycle_length
                    if prediction_time >= starttime:
                        prediction_trigger_times.update(
                            {c_index: prediction_time.strftime("%Y%m%d%H%M")}
                        )
                    else:
                        prediction_trigger_times.update(
                            {c_index: starttime.strftime("%Y%m%d%H%M")}
                        )

                    cycles.update(
                        {
                            c_index: {
                                "day": l_basetime.strftime("%Y%m%d"),
                                "time": l_basetime.strftime("%H%M"),
                                "validtime": l_basetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "basetime": l_basetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            }
                        }
                    )
                    logger.debug("Loop basetime: {}, fcint: {}", l_basetime, cycle)
                    logger.info(
                        "c_index={} prediction_trigger_times={}",
                        c_index,
                        prediction_trigger_times[c_index],
                    )
                    l_basetime = l_basetime + cycle
                if l_basetime >= endtime:
                    cont = False
            cont = False

        logger.debug("Built cycles: {}", cycles)

        comp_complete = None
        if config["compile.build"]:
            comp = EcflowSuiteFamily("Compilation", self.suite, self.ecf_files)
            EcflowSuiteTask(
                "CMakeBuild",
                comp,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
            )
            comp_complete = EcflowSuiteTrigger(comp, mode="complete")

        static_complete = None
        triggers = EcflowSuiteTriggers([comp_complete])
        if config["suite_control.create_static_data"]:
            static_data = EcflowSuiteFamily(
                "StaticData", self.suite, self.ecf_files, trigger=triggers
            )

            pgd_input = EcflowSuiteFamily("PgdInput", static_data, self.ecf_files)
            EcflowSuiteTask(
                "Gmted",
                pgd_input,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
                variables=None,
            )

            EcflowSuiteTask(
                "Soil",
                pgd_input,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
                variables=None,
            )

            pgd_trigger = EcflowSuiteTriggers([EcflowSuiteTrigger(pgd_input)])
            if self.one_decade:
                pgd_family = EcflowSuiteFamily(
                    "OfflinePgd",
                    static_data,
                    self.ecf_files,
                    trigger=pgd_trigger,
                    ecf_files_remotely=self.ecf_files_remotely,
                )
                decade_dates = get_decadal_list(
                    starttime,
                    endtime,
                )

                for dec_date in decade_dates:
                    decade_pgd_family = EcflowSuiteFamily(
                        f"decade_{get_decade(dec_date)}",
                        pgd_family,
                        self.ecf_files,
                        ecf_files_remotely=self.ecf_files_remotely,
                    )

                    EcflowSuiteTask(
                        "OfflinePgd",
                        decade_pgd_family,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        input_template=template,
                        variables={"ARGS": f"basetime={dec_date.isoformat()}"},
                        ecf_files_remotely=self.ecf_files_remotely,
                    )
            else:
                EcflowSuiteTask(
                    "OfflinePgd",
                    static_data,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": f"basetime={dec_date.isoformat()}"},
                    trigger=pgd_trigger,
                    ecf_files_remotely=self.ecf_files_remotely,
                )
            static_complete = EcflowSuiteTrigger(static_data)

        prep_complete = None
        days = []
        cycle_input_nodes = {}
        prediction_nodes = {}
        if config["suite_control.create_time_dependent_suite"]:
            cycles_values = cycles.values()
        else:
            cycles_values = []

        day_family = None
        time_trigger = None
        prev_cycle_input = None
        prev_initialization = None
        prev_prediction = None
        logger.info("cycles: {}", cycles)
        for cycle in cycles_values:
            cycle_day = cycle["day"]
            basetime = as_datetime(cycle["basetime"])
            c_index = basetime.strftime("%Y%m%d%H%M")
            time_variables = {
                "BASETIME": cycle["basetime"],
                "VALIDTIME": cycle["validtime"],
            }

            if cycle_day not in days:
                day_family = EcflowSuiteFamily(
                    cycle["day"],
                    self.suite,
                    self.ecf_files,
                    variables=time_variables,
                    ecf_files_remotely=self.ecf_files_remotely,
                )
                days.append(cycle_day)

            if (
                c_index in time_trigger_times
                and time_trigger_times[c_index] is not None
                and time_trigger_times[c_index] in cycle_input_nodes
            ):
                time_trigger = cycle_input_nodes[time_trigger_times[c_index]]

            logger.info("cycle_time={}", cycle["time"])
            triggers = EcflowSuiteTriggers([comp_complete, static_complete, time_trigger])

            time_family = EcflowSuiteFamily(
                cycle["time"],
                day_family,
                self.ecf_files,
                trigger=triggers,
                variables=time_variables,
                ecf_files_remotely=self.ecf_files_remotely,
            )
            cycle_input_nodes.update({c_index: EcflowSuiteTrigger(time_family)})

            prepare_cycle = EcflowSuiteTask(
                "PrepareCycle",
                time_family,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
            )
            prepare_cycle_complete = EcflowSuiteTrigger(prepare_cycle)

            triggers = EcflowSuiteTriggers(
                [comp_complete, static_complete, time_trigger, prepare_cycle_complete]
            )

            cycle_input = EcflowSuiteFamily(
                "CycleInput", time_family, self.ecf_files, trigger=triggers
            )

            # Analyse forcing
            do_an_forcing = config["an_forcing.enabled"]
            forecast_range = self.config["general.times.forecast_range"]
            do_forecast = False
            if as_timedelta(forecast_range).total_seconds() > 0.0:
                do_forecast = True

            if do_an_forcing:
                modes = ["an_forcing"]
                if do_forecast:
                    modes += ["default"]
            else:
                modes = ["default"]

            if config["suite_control.do_marsprep"]:
                mars_fam = EcflowSuiteFamily("mars", cycle_input, self.ecf_files)
                for mode in modes:
                    mode_mars = EcflowSuiteFamily(f"{mode}", mars_fam, self.ecf_files)
                    args = f"mode={mode};"
                    mars = EcflowSuiteTask(
                        "FetchMars",
                        mode_mars,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        input_template=template,
                        variables={"ARGS": args},
                    )
                    triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(mars)])

                if self.do_prep:
                    settings = SettingsFromNamelistAndConfig("prep", self.config)
                    cfile = settings.get_setting("NAM_PREP_SURF_ATM#CFILE")
                    cfiletype = settings.get_setting("NAM_PREP_SURF_ATM#CFILETYPE")
                    if cfile != "" and cfiletype == "GRIB":
                        mars_prep = EcflowSuiteTask(
                            "FetchMarsPrep",
                            mars_fam,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                            variables={"ARGS": "prep"},
                        )
                        triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(mars),
                                                        EcflowSuiteTrigger(mars_prep)])

            interpolate_bd = None
            if config["suite_control.interpolate2grid"]:
                interpolate_bd = EcflowSuiteFamily(
                    "interpolate_bd", cycle_input, self.ecf_files
                )
                for mode in modes:
                    mode_bd = EcflowSuiteFamily(f"{mode}", interpolate_bd, self.ecf_files)
                    args = f"mode={mode};"
                    interpolate2grid_fam = EcflowSuiteFamily(
                        "InterpolateBoundary",
                        mode_bd,
                        self.ecf_files,
                        trigger=triggers,
                        ecf_files_remotely=self.ecf_files_remotely,
                        variables={"ARGS": args},
                    )
                    fcint = as_timedelta(self.config["general.times.cycle_length"])
                    steps = int(int(fcint.total_seconds()) / 3600)
                    for bd in range(steps + 1):
                        bd_input = EcflowSuiteFamily(
                            f"bd_input{bd}",
                            interpolate2grid_fam,
                            self.ecf_files,
                            trigger=triggers,
                            variables={"ARGS": f"step={bd};mode={mode};"},
                            ecf_files_remotely=self.ecf_files_remotely,
                        )
                        EcflowSuiteTask(
                            "Interpolate2grid",
                            bd_input,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                            trigger=triggers,
                        )
                    triggers = EcflowSuiteTriggers(
                        [EcflowSuiteTrigger(interpolate2grid_fam)]
                    )

            forcing_trigger = triggers
            args = "mode=default;"
            forcing = EcflowSuiteTask(
                "Forcing",
                cycle_input,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
                variables={"ARGS": args},
                trigger=forcing_trigger,
            )
            triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(forcing)])
            mod_forcing = None
            if config["forcing.modify_forcing"]:
                mod_forcing = EcflowSuiteTask(
                    "ModifyForcing",
                    cycle_input,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": args},
                    trigger=triggers,
                )

            if mod_forcing is not None:
                triggers = EcflowSuiteTriggers(
                    [EcflowSuiteTrigger(forcing), EcflowSuiteTrigger(mod_forcing)]
                )

            # Create forcing for forecast
            if do_forecast:
                forecast_forcing = EcflowSuiteFamily(
                    "ForecastForcing", cycle_input, self.ecf_files, trigger=triggers
                )

                args = "mode=forecast;"
                forcing = EcflowSuiteTask(
                    "Forcing",
                    forecast_forcing,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": args},
                    trigger=forcing_trigger,
                )
                triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(forcing)])

            if do_an_forcing and self.do_prep:
                do_an_forcing = False

            if do_an_forcing:
                an_forcing = EcflowSuiteFamily(
                    "AnalyseForcing", cycle_input, self.ecf_files, trigger=triggers
                )

                analysis = EcflowSuiteFamily(
                    "Analysis", an_forcing, self.ecf_files, trigger=triggers
                )

                var_names = config["an_forcing.variables"]
                fcint = int(
                    as_timedelta(config["general.times.cycle_length"]).total_seconds()
                    / 3600
                )
                offsets = range(fcint + 1)
                for offset in offsets:
                    fg_trigger = triggers
                    offset_args = f"offset={offset};"
                    offset_fam = EcflowSuiteFamily(
                        f"offset{offset}",
                        analysis,
                        self.ecf_files,
                        trigger=fg_trigger,
                        variables={"ARGS": offset_args},
                    )
                    fg_offset = EcflowSuiteTask(
                        "FirstGuess4OI",
                        offset_fam,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        input_template=template,
                        variables={"ARGS": f"mode=an_forcing;{offset_args}"},
                    )

                    for var_name in var_names:
                        var_fam = EcflowSuiteFamily(
                            var_name,
                            offset_fam,
                            self.ecf_files,
                            variables={"ARGS": f"var_name={var_name};{offset_args}"},
                        )
                        qc = EcflowSuiteTask(
                            "QualityControl",
                            var_fam,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                            trigger=EcflowSuiteTriggers([EcflowSuiteTrigger(fg_offset)]),
                        )
                        EcflowSuiteTask(
                            "OptimalInterpolation",
                            var_fam,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                            trigger=EcflowSuiteTriggers([EcflowSuiteTrigger(qc)]),
                        )

                args = "mode=an_forcing;"
                forcing = EcflowSuiteTask(
                    "Forcing",
                    an_forcing,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": args},
                    trigger=EcflowSuiteTriggers([EcflowSuiteTrigger(analysis)]),
                )

                reforecast_trigger = EcflowSuiteTriggers([EcflowSuiteTrigger(an_forcing)])
                if prev_cycle_input is not None:
                    reforecast_trigger = EcflowSuiteTriggers(
                        [
                            EcflowSuiteTrigger(an_forcing),
                            EcflowSuiteTrigger(prev_cycle_input),
                        ]
                    )
                rerun_fam = EcflowSuiteFamily(
                    "ReForecast", cycle_input, self.ecf_files, trigger=reforecast_trigger
                )
                args = "mode=reforecast;"
                EcflowSuiteTask(
                    "OfflineForecast",
                    rerun_fam,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": args},
                    trigger=EcflowSuiteTriggers([EcflowSuiteTrigger(analysis)]),
                )

            logger.info(
                "c_index={} prediction_trigger_times[c_index]={}",
                c_index,
                prediction_trigger_times[c_index],
            )
            prediction_trigger = None
            if (
                c_index in prediction_trigger_times
                and prediction_trigger_times[c_index] in prediction_nodes
            ):
                prediction_trigger = prediction_nodes[prediction_trigger_times[c_index]]
            cycle_input_complete = EcflowSuiteTrigger(cycle_input)
            triggers = EcflowSuiteTriggers(
                [
                    static_complete,
                    prepare_cycle_complete,
                    cycle_input_complete,
                    prediction_trigger,
                ]
            )

            analysis = None
            do_soda = False
            do_initialization = False
            if self.do_prep:
                do_initialization = True
            else:
                settings = SettingsFromNamelistAndConfig("soda", config)

                schemes = {}
                schemes.update(
                    {"CASSIM_ISBA": settings.get_setting("NAM_ASSIM#CASSIM_ISBA")}
                )
                schemes.update(
                    {"CASSIM_SEA": settings.get_setting("NAM_ASSIM#CASSIM_SEA")}
                )
                schemes.update(
                    {"CASSIM_TEB": settings.get_setting("NAM_ASSIM#CASSIM_TEB")}
                )
                schemes.update(
                    {"CASSIM_WATER": settings.get_setting("NAM_ASSIM#CASSIM_WATER")}
                )

                do_soda = False
                for scheme in schemes.values():
                    if scheme.upper() != "NONE":
                        do_soda = True

                obs_types = settings.get_setting("NAM_OBS#COBS_M", default=[])
                nnco = settings.get_nnco(config, basetime=as_datetime(cycle["basetime"]))
                logger.info("nnco={}", nnco)
                if sum(nnco) > 0:
                    do_soda = True
                if do_soda:
                    do_initialization = True

            initialization = None
            if do_initialization:
                # Initialization
                initialization = EcflowSuiteFamily(
                    "Initialization", time_family, self.ecf_files, trigger=triggers
                )

            analysis = None
            if self.do_prep:
                prep = EcflowSuiteTask(
                    "OfflinePrep",
                    initialization,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                )
                prep_complete = EcflowSuiteTrigger(prep)
                # Might need an extra trigger for input

            else:
                triggers = EcflowSuiteTriggers(prep_complete)
                if do_soda:
                    perturbations = None
                    logger.debug("Perturbations: {}", schemes["CASSIM_ISBA"] == "EKF")
                    if schemes["CASSIM_ISBA"] == "EKF":
                        perturbations = EcflowSuiteFamily(
                            "Perturbations", initialization, self.ecf_files
                        )
                        nncv = settings.get_setting("NAM_VAR#NNCV")
                        names = settings.get_setting("NAM_VAR#CVAR_M")
                        llincheck = settings.get_setting("NAM_ASSIM#LLINCHECK")
                        triggers = None

                        name = "REF"
                        pert = EcflowSuiteFamily(name, perturbations, self.ecf_files)
                        args = f"pert=0;name={name};ivar=0"
                        logger.debug("args: {}", args)
                        variables = {"ARGS": args}
                        EcflowSuiteTask(
                            "PerturbedRun",
                            pert,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            trigger=triggers,
                            variables=variables,
                            input_template=template,
                        )

                        # Add extra families in case of llincheck
                        pert_signs = ["none"]
                        if llincheck:
                            pert_signs = ["pos", "neg"]

                        pert_families = []
                        for pert_sign in pert_signs:
                            if pert_sign == "none":
                                pert_families.append(perturbations)
                            elif pert_sign == "pos":
                                pert_families.append(
                                    EcflowSuiteFamily(
                                        "Pos",
                                        perturbations,
                                        self.ecf_files,
                                        variables=variables,
                                    )
                                )
                            elif pert_sign == "neg":
                                pert_families.append(
                                    EcflowSuiteFamily(
                                        "Neg",
                                        perturbations,
                                        self.ecf_files,
                                        variables=variables,
                                    )
                                )
                            else:
                                raise NotImplementedError

                        nivar = 1
                        for ivar, val in enumerate(nncv):
                            logger.debug("ivar {}, nncv[ivar] {}", str(ivar), str(val))
                            if val == 1:
                                name = names[ivar]
                                nfam = 0
                                for pert_parent in pert_families:
                                    pivar = str((nfam * len(nncv)) + ivar + 1)
                                    pert = EcflowSuiteFamily(
                                        name, pert_parent, self.ecf_files
                                    )
                                    pert_sign = pert_signs[nfam]
                                    args = f"pert={pivar!s};name={name};ivar={nivar!s};"
                                    args += f"pert_sign={pert_sign}"
                                    logger.debug("args: {}", args)
                                    variables = {"ARGS": args}
                                    EcflowSuiteTask(
                                        "PerturbedRun",
                                        pert,
                                        config,
                                        self.task_settings,
                                        self.ecf_files,
                                        trigger=triggers,
                                        variables=variables,
                                        input_template=template,
                                    )
                                    nfam += 1  # noqa SIM113
                                nivar = nivar + 1

                    prepare_oi_soil_input = None
                    prepare_oi_climate = None
                    if schemes["CASSIM_ISBA"] == "OI":
                        prepare_oi_soil_input = EcflowSuiteTask(
                            "PrepareOiSoilInput",
                            initialization,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                        )
                        prepare_oi_climate = EcflowSuiteTask(
                            "PrepareOiClimate",
                            initialization,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                        )

                    prepare_sst = None
                    if schemes["CASSIM_ISBA"] == "INPUT" and settings.setting_is(
                        "NAM_ASSIM#CFILE_FORMAT_SST", "ASCII"
                    ):
                        prepare_sst = EcflowSuiteTask(
                            "PrepareSST",
                            initialization,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                        )

                    an_variables = {"t2m": False, "rh2m": False, "sd": False}
                    obs_types = settings.get_setting("NAM_OBS#COBS_M")
                    nnco = settings.get_nnco(
                        config, basetime=as_datetime(cycle["basetime"])
                    )
                    need_obs = False
                    for t_ind, val in enumerate(obs_types):
                        if nnco[t_ind] == 1:
                            if val in ("T2M", "T2M_P"):
                                an_variables.update({"t2m": True})
                                need_obs = True
                            elif val in ("HU2M", "HU2M_P"):
                                an_variables.update({"rh2m": True})
                                need_obs = True
                            elif val == "SWE":
                                an_variables.update({"sd": True})
                                need_obs = True

                    analysis = EcflowSuiteFamily(
                        "Analysis", initialization, self.ecf_files
                    )
                    fg4oi = EcflowSuiteTask(
                        "FirstGuess4OI",
                        analysis,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        input_template=template,
                        variables={"ARGS": "mode=analysis;"},
                    )
                    fg4oi_complete = EcflowSuiteTrigger(fg4oi)

                    cryo_obs_sd = config["observations.cryo_obs_sd"]
                    cryo2json_complete = fg4oi_complete
                    if cryo_obs_sd:
                        cryo_trigger = EcflowSuiteTriggers(fg4oi_complete)
                        cryo2json = EcflowSuiteTask(
                            "CryoClim2json",
                            analysis,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            trigger=cryo_trigger,
                            input_template=template,
                        )
                        cryo2json_complete = EcflowSuiteTrigger(cryo2json)

                    fetchobs_complete = None
                    if self.has_mars and need_obs:
                        fetchobs = EcflowSuiteTask(
                            "FetchMarsObs",
                            analysis,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            input_template=template,
                        )
                        fetchobs_complete = EcflowSuiteTrigger(fetchobs)

                    triggers = []
                    for var, active in an_variables.items():
                        if active:
                            variables = {"ARGS": f"var_name={var};"}
                            an_var_fam = EcflowSuiteFamily(
                                var, analysis, self.ecf_files, variables=variables
                            )
                            qc_triggers = None
                            if var == "sd":
                                qc_triggers = EcflowSuiteTriggers(
                                    [
                                        fg4oi_complete,
                                        fetchobs_complete,
                                        cryo2json_complete,
                                    ]
                                )
                            else:
                                qc_triggers = EcflowSuiteTriggers(fetchobs_complete)
                            qc_task = EcflowSuiteTask(
                                "QualityControl",
                                an_var_fam,
                                config,
                                self.task_settings,
                                self.ecf_files,
                                trigger=qc_triggers,
                                input_template=template,
                            )
                            oi_triggers = EcflowSuiteTriggers(
                                [EcflowSuiteTrigger(qc_task), EcflowSuiteTrigger(fg4oi)]
                            )
                            EcflowSuiteTask(
                                "OptimalInterpolation",
                                an_var_fam,
                                config,
                                self.task_settings,
                                self.ecf_files,
                                trigger=oi_triggers,
                                input_template=template,
                            )
                            triggers.append(EcflowSuiteTrigger(an_var_fam))

                    oi2soda_complete = None
                    if len(triggers) > 0:
                        triggers = EcflowSuiteTriggers(triggers)
                        oi2soda = EcflowSuiteTask(
                            "Oi2soda",
                            analysis,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            trigger=triggers,
                            input_template=template,
                        )
                        oi2soda_complete = EcflowSuiteTrigger(oi2soda)

                    prepare_lsm = None
                    need_lsm = False
                    if schemes["CASSIM_ISBA"] == "OI":
                        need_lsm = True
                    if settings.setting_is("NAM_ASSIM#LEXTRAP_WATER", True):
                        need_lsm = True
                    if need_lsm:
                        triggers = EcflowSuiteTriggers(fg4oi_complete)
                        prepare_lsm = EcflowSuiteTask(
                            "PrepareLSM",
                            initialization,
                            config,
                            self.task_settings,
                            self.ecf_files,
                            trigger=triggers,
                            input_template=template,
                        )

                    triggers = [oi2soda_complete]
                    if perturbations is not None:
                        triggers.append(EcflowSuiteTrigger(perturbations))
                    if prepare_oi_soil_input is not None:
                        triggers.append(EcflowSuiteTrigger(prepare_oi_soil_input))
                    if prepare_oi_climate is not None:
                        triggers.append(EcflowSuiteTrigger(prepare_oi_climate))
                    if prepare_sst is not None:
                        triggers.append(EcflowSuiteTrigger(prepare_sst))
                    if prepare_lsm is not None:
                        triggers.append(EcflowSuiteTrigger(prepare_lsm))

                    triggers = EcflowSuiteTriggers(triggers)
                    EcflowSuiteTask(
                        "Soda",
                        analysis,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        trigger=triggers,
                        input_template=template,
                    )

            triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(cycle_input)])
            if initialization is not None:
                triggers.add_triggers(EcflowSuiteTrigger(initialization))
            do_prediction = True
            if not do_forecast:
                if basetime == endtime:
                    do_prediction = False
                if config["an_forcing.enabled"]:
                    do_prediction = False

            prediction = None
            if do_prediction:
                if prev_cycle_input is not None:
                    triggers.add_triggers(EcflowSuiteTrigger(prev_cycle_input))
                if prev_initialization is not None:
                    triggers.add_triggers(EcflowSuiteTrigger(prev_initialization))
                if prev_prediction is not None:
                    triggers.add_triggers(EcflowSuiteTrigger(prev_prediction))
                prediction = EcflowSuiteFamily(
                    "Prediction", time_family, self.ecf_files, trigger=triggers
                )
                prediction_nodes.update({c_index: EcflowSuiteTrigger(prediction)})

            if basetime != endtime and not do_an_forcing:
                cycle_forecast = EcflowSuiteFamily(
                    "Cycle", prediction, self.ecf_files, trigger=triggers
                )
                EcflowSuiteTask(
                    "OfflineForecast",
                    cycle_forecast,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": "mode=cycle;"},
                )
            if do_forecast:
                prediction = EcflowSuiteFamily(
                    "LongForecast", prediction, self.ecf_files, trigger=triggers
                )
                long_forecast = EcflowSuiteTask(
                    "OfflineForecast",
                    prediction,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    variables={"ARGS": "mode=forecast;"},
                )
                triggers = EcflowSuiteTriggers(EcflowSuiteTrigger(long_forecast))

            if prediction is None:
                if initialization is None:
                    triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(cycle_input)])
                else:
                    triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(initialization)])
            else:
                triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(prediction)])

            verification_fam = None
            do_verification = False
            offline_settings = SettingsFromNamelistAndConfig("offline", config)
            if config["suite_control.do_verification"]:
                do_verification = False
                modes = ["cycle"]
                if do_forecast:
                    modes += ["forecast"]
                for mode in modes:
                    with contextlib.suppress(KeyError):
                        ver_vars = config[f"verification.{mode}.variables"]
                        if len(ver_vars) > 0:
                            do_verification = True
                # Skip verfication when doinf prep and not forecast
                if not do_forecast and self.do_prep:
                    do_verification = False

            do_postproc = False
            if do_verification:
                do_postproc = True
            if do_an_forcing or analysis:
                do_postproc = True

            pp_fam = None
            if do_postproc:
                pp_fam = EcflowSuiteFamily(
                    "PostProcessing", time_family, self.ecf_files, trigger=triggers
                )

            if do_verification:
                verification_fam = EcflowSuiteFamily(
                    "Verification", pp_fam, self.ecf_files
                )
                modes = ["cycle"]
                if do_forecast:
                    modes += ["forecast"]
                for mode in modes:
                    try:
                        ver_vars = config[f"verification.{mode}.variables"]
                    except KeyError:
                        ver_vars = []

                    if len(ver_vars) > 0:
                        if mode == "cycle":
                            forecast_range = as_timedelta(
                                config["general.times.cycle_length"]
                            )
                        elif mode == "forecast":
                            forecast_range = as_timedelta(
                                config["general.times.forecast_range"]
                            )
                        extra = ""
                        if mode == "forecast":
                            extra = ".forecast"
                        try:
                            output_frequency = self.config[
                                f"offline{extra}.args.output-frequency"
                            ]
                        except KeyError:
                            try:
                                output_frequency = offline_settings.nml["nam_io_offline"][
                                    "xtstep_output"
                                ]
                            except KeyError:
                                raise RuntimeError(
                                    "No value for output-frequency or xtstep_output found"
                                ) from KeyError

                        logger.info("mode={} output_frequency={}", mode, output_frequency)
                        if output_frequency is not None:
                            verification_mode_fam = EcflowSuiteFamily(
                                mode, verification_fam, self.ecf_files
                            )
                            vbasetime = basetime
                            if mode == "cycle" and self.config["an_forcing.enabled"]:
                                vbasetime = basetime - forecast_range
                            iso_basetime = vbasetime.isoformat()
                            dt = as_timedelta(f"PT{int(output_frequency)}S")
                            validtime = vbasetime + dt
                            block = 0
                            while validtime <= vbasetime + forecast_range:
                                iso_validtime = validtime.isoformat()
                                block += 1
                                verification_mode_lt_fam = EcflowSuiteFamily(
                                    f"output{block}",
                                    verification_mode_fam,
                                    self.ecf_files,
                                )
                                for var_name in ver_vars:
                                    args = (
                                        f"mode={mode};var_name={var_name};"
                                        + f"basetime={iso_basetime};"
                                        + f"validtime={iso_validtime};"
                                    )
                                    variables = {"ARGS": args}
                                    harp_fam = EcflowSuiteFamily(
                                        var_name,
                                        verification_mode_lt_fam,
                                        self.ecf_files,
                                        variables=variables,
                                    )
                                    EcflowSuiteTask(
                                        "HarpSQLite",
                                        harp_fam,
                                        config,
                                        self.task_settings,
                                        self.ecf_files,
                                        input_template=template,
                                    )
                                validtime = validtime + dt

            qc2obsmon_fam = None
            if do_an_forcing or analysis:
                qc2obsmon_fam = EcflowSuiteFamily(
                    "QC2Obsmon", pp_fam, self.ecf_files, trigger=triggers
                )
            if do_an_forcing:
                fcint = int(
                    as_timedelta(config["general.times.cycle_length"]).total_seconds()
                    / 3600
                )
                offsets = range(fcint + 1)

                if len(offsets) > 0:
                    qc2obsmon_an_forcing = EcflowSuiteFamily(
                        "an_forcing", qc2obsmon_fam, self.ecf_files, trigger=triggers
                    )
                    if do_an_forcing:
                        for offset in offsets:
                            offset_fam = EcflowSuiteFamily(
                                f"offset{offset}",
                                qc2obsmon_an_forcing,
                                self.ecf_files,
                                trigger=triggers,
                                variables={"ARGS": f"offset={offset};mode=an_forcing;"},
                            )
                            EcflowSuiteTask(
                                "Qc2obsmon",
                                offset_fam,
                                config,
                                self.task_settings,
                                self.ecf_files,
                                input_template=template,
                            )
            if analysis:
                qc2obsmon_analysis_fam = EcflowSuiteFamily(
                    "analysis",
                    qc2obsmon_fam,
                    self.ecf_files,
                    trigger=triggers,
                    variables={"ARGS": "mode=default;"},
                )
                EcflowSuiteTask(
                    "Qc2obsmon",
                    qc2obsmon_analysis_fam,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                )

            trigger = None
            if pp_fam is not None:
                trigger = EcflowSuiteTriggers(EcflowSuiteTrigger(pp_fam))
            cday = cycle["day"]
            ctime = cycle["time"]
            task_logs = config["system.wrk"]
            args = ";".join(
                [
                    f"joboutdir={self.ecf_out}/{self.name}/{cday}/{ctime}",
                    f"tarname={self.name}_{cday}{ctime}",
                    f"task_logs={task_logs}",
                    "config_label=hourlogs",
                ]
            )
            variables = {"ARGS": args}

            EcflowSuiteTask(
                "CollectLogs",
                time_family,
                config,
                self.task_settings,
                self.ecf_files,
                trigger=trigger,
                variables=variables,
                input_template=template,
            )

            if cycle_input is not None:
                prev_cycle_input = cycle_input
            if initialization is not None:
                prev_initialization = initialization
            if prediction is not None:
                prev_prediction = prediction

            # For now set do_prep False after for next cycles and do cycling
            self.do_prep = False
