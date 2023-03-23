"""Job submission setup."""
import collections.abc
import json
import logging
import os
import subprocess  # noqa S404
import sys

from ..tasks.discover_tasks import get_task


class TaskSettings(object):
    """Set the task specific setttings."""

    def __init__(self, config):
        """Construct the task specific settings.

        Args:
            config (ParsedConfig): Parsed config

        """
        self.submission_defs = config.get_value("submission").dict()
        self.job_type = None

    @staticmethod
    def _update_task_setting(dic, upd):
        for key, val in upd.items():
            if isinstance(val, collections.abc.Mapping):
                dic[key] = TaskSettings._update_task_setting(dic.get(key, {}), val)
            else:
                logging.debug("key=%s value=%s", key, val)
                dic[key] = val
        return dic

    def parse_submission_defs(self, task):
        """Parse the submssion definitions.

        Args:
            task (str): The name of the task

        Returns:
            dict: Parsed settings

        """
        task_settings = {"BATCH": {}, "ENV": {}}
        all_defs = self.submission_defs
        submit_types = all_defs["submit_types"]
        default_submit_type = all_defs["default_submit_type"]
        task_submit_type = None
        for s_t in submit_types:
            if s_t in all_defs and "tasks" in all_defs[s_t]:
                for tname in all_defs[s_t]["tasks"]:
                    if tname == task:
                        task_submit_type = s_t
        if task_submit_type is None:
            task_submit_type = default_submit_type

        if task_submit_type in all_defs:
            for __ in all_defs[task_submit_type]:
                logging.debug("task_submit_type for task %s: %s", task, task_submit_type)
                task_settings = self._update_task_setting(
                    task_settings, all_defs[task_submit_type]
                )

        if "task_exceptions" in all_defs:
            if task in all_defs["task_exceptions"]:
                logging.debug("Task task_exceptions for task %s", task)
                task_settings = self._update_task_setting(
                    task_settings, all_defs["task_exceptions"][task]
                )

        if "SCHOST" in task_settings:
            self.job_type = task_settings["SCHOST"]

        logging.debug("Task settings for task %s: %s", task, task_settings)
        return task_settings

    def get_task_settings(self, task, key=None, variables=None, ecf_micro="%"):
        """Get task settings.

        Args:
            task (_type_): _description_
            key (_type_, optional): _description_. Defaults to None.
            variables (_type_, optional): _description_. Defaults to None.
            ecf_micro (str, optional): _description_. Defaults to "%".

        Returns:
            _type_: _description_
        """
        task_settings = self.parse_submission_defs(task)
        if key is None:
            return task_settings
        else:
            if key in task_settings:
                m_task_settings = {}
                logging.debug(type(task_settings[key]))
                if isinstance(task_settings[key], dict):
                    for setting, value in task_settings[key].items():
                        logging.debug("%s %s variables: %s", setting, value, variables)
                        if variables is not None:
                            if setting in variables:
                                value = f"{ecf_micro}{setting}{ecf_micro}"
                                logging.debug(value)
                        if isinstance(value, str):
                            value = value.replace("@NAME@", task)
                        m_task_settings.update({setting: value})
                    logging.debug(m_task_settings)
                    return m_task_settings
                else:
                    value = task_settings[key]
                    if variables is not None:
                        if key in variables:
                            value = f"{ecf_micro}{variables[setting]}{ecf_micro}"
                    return value
            return None

    def recursive_items(self, dictionary):
        """Recursive loop of dict.

        Args:
            dictionary (_type_): _description_

        Yields:
            _type_: _description_
        """
        for key, value in dictionary.items():
            if isinstance(value, dict):
                yield (key, value)
                yield from self.recursive_items(value)
            else:
                yield (key, value)

    def get_settings(self, task):
        """Get the settings.

        Args:
            task (_type_): _description_

        Returns:
            _type_: _description_
        """
        settings = {}
        task_settings = self.parse_submission_defs(task)
        keys = []
        for key, value in self.recursive_items(task_settings):
            if isinstance(value, str):
                logging.debug(key)
                keys.append(key)
        logging.debug(keys)
        for key, value in self.recursive_items(task_settings):
            logging.debug("key=%s value=%s", key, value)
            if key in keys:
                logging.debug("update %s %s", key, value)
                settings.update({key: value})
        return settings

    def parse_job(
        self, task, config, input_template_job, task_job, variables=None, ecf_micro="%"
    ):
        """Read default job and change interpretor.

        Args:
            task (str): Task name
            config (experiment.Configuration): The configuration
            input_template_job (str): Input container template.
            task_job (str): Task container
            variables (_type_, optional): _description_. Defaults to None.
            ecf_micro (str, optional): _description_.

        """
        interpreter = self.get_task_settings(task, "INTERPRETER")
        logging.debug(interpreter)
        if interpreter is None:
            interpreter = f"#!{sys.executable}"

        logging.debug(interpreter)
        with open(input_template_job, mode="r", encoding="utf-8") as file_handler:
            input_content = file_handler.read()
        dir_name = os.path.dirname(os.path.realpath(task_job))
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        with open(task_job, mode="w", encoding="utf-8") as file_handler:
            file_handler.write(f"{interpreter}\n")
            batch_settings = self.get_task_settings(
                task, "BATCH", variables=variables, ecf_micro=ecf_micro
            )
            logging.debug("batch settings %s", batch_settings)
            for __, b_setting in batch_settings.items():
                file_handler.write(f"{b_setting}\n")
            env_settings = self.get_task_settings(
                task, "ENV", variables=variables, ecf_micro=ecf_micro
            )
            logging.debug("Env settings for task %s %s", task, env_settings)
            logging.debug("Variables for task %s %s", task, variables)
            python_task_env = ""
            for __, e_setting in env_settings.items():
                python_task_env = python_task_env + f"{e_setting}\n"
            input_content = input_content.replace("# @ENV_SUB1@", python_task_env)
            input_content = input_content.replace("@STAND_ALONE_TASK_NAME@", task)
            # TODO
            try:
                config_file = config.get_value("metadata.source_file_path")
            except AttributeError:
                config_file = None
            if config_file is not None:
                input_content = input_content.replace(
                    "@STAND_ALONE_TASK_CONFIG@", str(config_file)
                )
            loglevel = self.get_task_settings(task, "LOGLEVEL")
            if loglevel is None:
                loglevel = "INFO"
            input_content = input_content.replace("@STAND_ALONE_TASK_LOGLEVEL@", loglevel)
            file_handler.write(input_content)
        # Make file executable for user
        os.chmod(task_job, 0o744)


class TaskSettingsJson(TaskSettings):
    """Set the task specific setttings."""

    def __init__(self, submission_defs_file):
        """Construct the task specific settings.

        Args:
            submission_defs_file (str): Submission definition json file
        """
        with open(submission_defs_file, mode="r", encoding="utf-8") as file_handler:
            submission_defs = json.load(file_handler)
        TaskSettings.__init__(self, submission_defs)


class NoSchedulerSubmission:
    """Create and submit job without a scheduler."""

    def __init__(self, task_settings):
        """Construct the task specific settings.

        Args:
             task_settings (dict): Submission definitions
        """
        self.task_settings = task_settings

    def submit(self, task, config, template_job, task_job, output):
        """Submit task.

        Args:
            task (str): Task name
            config (deode.ParsedConfig): Config
            template_job (str): Task template job file
            task_job (str): Task job file
            output(str): Output file

        Raises:
            KeyError: Task is not found
            RuntimeError: Submission failure

        """
        try:
            get_task(task, config)
        except KeyError:
            raise KeyError(f"Task not found: {task}") from KeyError

        self.task_settings.parse_job(task, config, template_job, task_job)

        troika = TroikaSettings(config)
        cmd = (
            f"{troika.command} -c {troika.config} submit {self.task_settings.job_type} "
            f"{task_job} -o {output}"
        )
        try:
            subprocess.check_call(cmd.split())  # noqaS601
        except Exception as exc:
            raise RuntimeError(f"Submission failed with {repr(exc)}") from exc


class TroikaSettings:
    """Group troika settings."""

    def __init__(self, config):
        """Construct the troika relevant settings.

        Args:
            config (ParsedConfig): Parsed config

        """
        self.command = config.get_value("troika.command")
        self.config = config.get_value("troika.config")
