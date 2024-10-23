"""Offline suite."""
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

from surfexp.experiment import get_nnco, get_total_unique_cycle_list, setting_is


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

        realization = None
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
        logger.debug("DTGSTART: {} DTGBEG: {} DTGEND: {}", basetime, starttime, endtime)

        l_basetime = basetime
        logger.debug("Building list of DTGs")

        cycles = {}
        time_trigger_times = {}
        prediction_trigger_times = {}

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
            if config["compile.cmake"]:
                EcflowSuiteTask(
                    "CMakeBuild",
                    comp,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                )
                comp_complete = EcflowSuiteTrigger(comp, mode="complete")
            else:
                sync = EcflowSuiteTask(
                    "SyncSourceCode",
                    comp,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                )
                sync_complete = EcflowSuiteTrigger(sync, mode="complete")
                configure = EcflowSuiteTask(
                    "ConfigureOfflineBinaries",
                    comp,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    trigger=EcflowSuiteTriggers([sync_complete]),
                )
                configure_complete = EcflowSuiteTrigger(configure, mode="complete")
                EcflowSuiteTask(
                    "MakeOfflineBinaries",
                    comp,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    trigger=EcflowSuiteTriggers([configure_complete]),
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
                        variables={"ARGS": f"basetime={dec_date}"},
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
                    variables={"ARGS": f"basetime={basetime}"},
                    trigger=pgd_trigger,
                    ecf_files_remotely=self.ecf_files_remotely,
                )
            static_complete = EcflowSuiteTrigger(static_data)

        prep_complete = None
        days = []
        cycle_input_nodes = {}
        prediction_nodes = {}
        for cycle in cycles.values():
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

            time_trigger = None
            if (
                c_index in time_trigger_times
                and time_trigger_times[c_index] is not None
                and time_trigger_times[c_index] in cycle_input_nodes
            ):
                time_trigger = cycle_input_nodes[time_trigger_times[c_index]]

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

            triggers.add_triggers([prepare_cycle_complete])

            cycle_input = EcflowSuiteFamily(
                "CycleInput", time_family, self.ecf_files, trigger=triggers
            )

            forcing = EcflowSuiteTask(
                "Forcing",
                cycle_input,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
            )
            triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(forcing)])
            if config["forcing.modify_forcing"]:
                EcflowSuiteTask(
                    "ModifyForcing",
                    cycle_input,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                    trigger=triggers,
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
            triggers = EcflowSuiteTriggers(
                [static_complete, prepare_cycle_complete, prediction_trigger]
            )

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
                schemes = config["SURFEX.ASSIM.SCHEMES"].dict()
                do_soda = False
                for scheme in schemes:
                    if schemes[scheme].upper() != "NONE":
                        do_soda = True

                obs_types = config["SURFEX.ASSIM.OBS.COBS_M"]
                nnco = get_nnco(config, basetime=as_datetime(cycle["basetime"]))
                for ivar, val in enumerate(nnco):
                    if val == 1 and obs_types[ivar] == "SWE":
                        do_soda = True

                triggers = EcflowSuiteTriggers(prep_complete)
                if not do_soda:
                    EcflowSuiteTask(
                        "CycleFirstGuess",
                        initialization,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        trigger=triggers,
                        input_template=template,
                    )
                else:
                    fg_task = EcflowSuiteTask(
                        "FirstGuess",
                        initialization,
                        config,
                        self.task_settings,
                        self.ecf_files,
                        trigger=triggers,
                        input_template=template,
                    )

                    perturbations = None
                    logger.debug(
                        "Perturbations: {}",
                        setting_is(
                            config,
                            "SURFEX.ASSIM.SCHEMES.ISBA",
                            "EKF",
                            realization=realization,
                        ),
                    )
                    if setting_is(
                        config,
                        "SURFEX.ASSIM.SCHEMES.ISBA",
                        "EKF",
                        realization=realization,
                    ):
                        perturbations = EcflowSuiteFamily(
                            "Perturbations", initialization, self.ecf_files
                        )
                        nncv = config["SURFEX.ASSIM.ISBA.EKF.NNCV"]
                        names = config["SURFEX.ASSIM.ISBA.EKF.CVAR_M"]
                        llincheck = config["SURFEX.ASSIM.ISBA.EKF.LLINCHECK"]
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
                                    nfam += 1
                                nivar = nivar + 1

                    prepare_oi_soil_input = None
                    prepare_oi_climate = None
                    if setting_is(config, "SURFEX.ASSIM.SCHEMES.ISBA", "OI"):
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
                    if setting_is(
                        config, "SURFEX.ASSIM.SCHEMES.SEA", "INPUT"
                    ) and setting_is(
                        config, "SURFEX.ASSIM.SEA.CFILE_FORMAT_SST", "ASCII"
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
                    obs_types = config["SURFEX.ASSIM.OBS.COBS_M"]
                    nnco = get_nnco(config, basetime=as_datetime(cycle["basetime"]))
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
                    if setting_is(config, "SURFEX.ASSIM.SCHEMES.ISBA", "OI"):
                        need_lsm = True
                    if (
                        setting_is(config, "SURFEX.ASSIM.SCHEMES.INLAND_WATER", "WATFLX")
                        and config["SURFEX.ASSIM.INLAND_WATER.LEXTRAP_WATER"]
                    ):
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

                    triggers = [EcflowSuiteTrigger(fg_task), oi2soda_complete]
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

            self.do_prep = False
            triggers = EcflowSuiteTriggers(
                [EcflowSuiteTrigger(cycle_input), EcflowSuiteTrigger(initialization)]
            )
            prediction = EcflowSuiteFamily(
                "Prediction", time_family, self.ecf_files, trigger=triggers
            )
            prediction_nodes.update({c_index: EcflowSuiteTrigger(prediction)})

            forecast = EcflowSuiteTask(
                "OfflineForecast",
                prediction,
                config,
                self.task_settings,
                self.ecf_files,
                input_template=template,
            )
            triggers = EcflowSuiteTriggers(EcflowSuiteTrigger(forecast))
            EcflowSuiteTask(
                "LogProgress",
                prediction,
                config,
                self.task_settings,
                self.ecf_files,
                trigger=triggers,
                input_template=template,
            )

            triggers = EcflowSuiteTriggers([EcflowSuiteTrigger(prediction)])

            pp_fam = EcflowSuiteFamily(
                "PostProcessing", time_family, self.ecf_files, trigger=triggers
            )

            log_pp_trigger = None
            if analysis is not None:
                qc2obsmon = EcflowSuiteTask(
                    "Qc2obsmon",
                    pp_fam,
                    config,
                    self.task_settings,
                    self.ecf_files,
                    input_template=template,
                )
                trigger = EcflowSuiteTrigger(qc2obsmon)
                log_pp_trigger = EcflowSuiteTriggers(trigger)

            EcflowSuiteTask(
                "LogProgressPP",
                pp_fam,
                config,
                self.task_settings,
                self.ecf_files,
                trigger=log_pp_trigger,
                input_template=template,
            )
