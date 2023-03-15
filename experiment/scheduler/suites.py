"""Ecflow suites."""
import os
import sys
import logging
try:
    import ecflow  # noqa reportMissingImports
except ImportError:
    ecflow = None


class EcflowNode():
    """A Node class is the abstract base class for Suite, Family and Task.

    Every Node instance has a name, and a path relative to a suite
    """

    def __init__(self, name, node_type, parent, ecf_files, variables=None, triggers=None,
                 def_status=None):
        """Construct the EcflowNode.

        Args:
            name (str): Name of node
            node_type (str): Node type
            parent (EcflowNode): Parent node
            ecf_files (str): Location of ecf files
            variables (dict, optional): Variables to map. Defaults to None
            triggers (): Triggers. Defaults to None
            def_status (str, optional): Default status. Defaults to False.
        Raises:
            NotImplementedError: Node type not implemented

        """
        self.name = name
        self.node_type = node_type

        has_node = True
        if parent is None:
            has_node = False
        if has_node and node_type != "suite":
            if hasattr(parent, "ecf_node"):
                if parent.ecf_node is None:
                    has_node = False
        if not has_node:
            self.ecf_node = None
            path = ""
        else:
            if self.node_type == "family":
                self.ecf_node = parent.ecf_node.add_family(self.name)
            elif self.node_type == "task":
                self.ecf_node = parent.ecf_node.add_task(self.name)
            elif self.node_type == "suite":
                self.ecf_node = parent.add_suite(self.name)
            else:
                raise NotImplementedError

            path = self.ecf_node.get_abs_node_path()

        self.path = path
        self.ecf_container_path = ecf_files + self.path
        if variables is not None:
            for key, value in variables.items():
                logging.debug("key=%s value=%s", key, value)
                if self.ecf_node is not None:
                    self.ecf_node.add_variable(key, value)

        if triggers is not None:
            if isinstance(triggers, EcflowSuiteTriggers):
                if triggers.trigger_string is not None:
                    self.ecf_node.add_trigger(triggers.trigger_string)
                else:
                    logging.warning("WARNING: Empty trigger")
            else:
                raise Exception("Triggers must be a Triggers object")
        self.triggers = triggers

        if def_status is not None:
            if isinstance(def_status, str):
                self.ecf_node.add_defstatus(ecflow.Defstatus(def_status))
            elif isinstance(def_status, ecflow.Defstatus):
                self.ecf_node.add_defstatus(def_status)
            else:
                raise Exception("Unknown defstatus")

    def add_part_trigger(self, triggers, mode=True):
        """Add a part trigger.

        Args:
            triggers (_type_): _description_
            mode (bool, optional): _description_. Defaults to True.

        Raises:
            Exception: _description_

        """
        if isinstance(triggers, EcflowSuiteTriggers):
            if triggers.trigger_string is not None:
                self.ecf_node.add_part_trigger(triggers.trigger_string, mode)
            else:
                logging.warning("WARNING: Empty trigger")
        else:
            raise Exception("Triggers must be a Triggers object")


class EcflowNodeContainer(EcflowNode):
    """Ecflow node container.

    Args:
        EcflowNode (EcflowNode): Parent class.
    """

    def __init__(self, name, node_type, parent, ecf_files, variables=None,
                 triggers=None, def_status=None):
        """Construct EcflowNodeContainer.

        Args:
            name (str): Name of the node container.
            node_type (str): What kind of node.
            parent (EcflowNode): Parent to this node.
            ecf_files (str): Location of ecf files
            variables (dict, optional): Variables to map. Defaults to None
            triggers (): Triggers. Defaults to None
            def_status (str, optional): Default status. Defaults to False.
        """
        EcflowNode.__init__(
            self, name, node_type, parent, variables=variables, ecf_files=ecf_files,
            triggers=triggers, def_status=def_status
        )


class EcflowSuite(EcflowNodeContainer):
    """EcflowSuite.

    Args:
        EcflowNodeContainer
        (EcflowNodeContainer): A child of the EcflowNodeContainer class.
    """

    def __init__(self, name, ecf_files, variables=None, def_status=None,
                 triggers=None, dry_run=False):
        """Construct the Ecflow suite.

        Args:
            name (str): Name of suite
            ecf_files (str): Location of ecf files
            variables (dict, optional): Variables to map. Defaults to None
            def_status (str, optional): Default status. Defaults to False.
            dry_run (bool, optional): Dry run not using ecflow. Defaults to False.
            triggers (): Triggers. Defaults to None
            def_status (str, optional): Default status. Defaults to False.

        """
        if dry_run:
            self.defs = None
        else:
            self.defs = ecflow.Defs({})

        EcflowNodeContainer.__init__(
            self, name, "suite", self.defs, ecf_files, variables=variables,
            def_status=def_status
        )

    def save_as_defs(self, def_file):
        """Save defintion file.

        Args:
            def_file (str): Name of the definition file.
        """
        if self.defs is not None:
            self.defs.save_as_defs(def_file)
        logging.info("def file saved to %s", def_file)


class EcflowSuiteTriggers():
    """Triggers to an ecflow suite."""

    def __init__(self, triggers, **kwargs):
        """Construct EcflowSuiteTriggers.

        Args:
            triggers (list): List of EcflowSuiteTrigger objects.

        """
        mode = kwargs.get("mode")
        if mode is None:
            mode = "AND"

        trigger_string = self.create_string(triggers, mode)
        self.trigger_string = trigger_string

    @staticmethod
    def create_string(triggers, mode):
        """Create the trigger string.

        Args:
            triggers (list): List of trigger objects
            mode     (str): Concatenation type.

        Raises:
            Exception: _description_
            Exception: _description_

        Returns:
            str: The trigger string based on trigger objects.

        """
        if not isinstance(triggers, list):
            triggers = [triggers]

        if len(triggers) == 0:
            raise Exception

        trigger_string = "("
        first = True
        for trigger in triggers:
            if trigger is not None:
                cat = ""
                if not first:
                    cat = " " + mode + " "
                if isinstance(trigger, EcflowSuiteTriggers):
                    trigger_string = trigger_string + cat + trigger.trigger_string
                else:
                    if isinstance(trigger, EcflowSuiteTrigger):
                        trigger_string = trigger_string + cat + trigger.node.path + " == " +\
                                                                                    trigger.mode
                    else:
                        raise Exception("Trigger must be a Trigger object")
                first = False
        trigger_string = trigger_string + ")"
        # If no triggers were found/set
        if first:
            trigger_string = None
        return trigger_string

    def add_triggers(self, triggers, mode="AND"):
        """Add triggers.

        Args:
            triggers (EcflowSuiteTriggers): The triggers
            mode (str, optional): Cat mode. Defaults to "AND".

        """
        cat_string = " " + mode + " "
        trigger_string = self.create_string(triggers, mode)
        if trigger_string is not None:
            self.trigger_string = self.trigger_string + cat_string + trigger_string


class EcflowSuiteTrigger():
    """EcFlow Trigger in a suite."""

    def __init__(self, node, mode="complete"):
        """Create a EcFlow trigger object.

        Args:
            node (scheduler.EcflowNode): The node to trigger on
            mode (str):

        """
        self.node = node
        self.mode = mode


class EcflowSuiteFamily(EcflowNodeContainer):
    """A family in ecflow.

    Args:
        EcflowNodeContainer (_type_): _description_
    """

    def __init__(self, name, parent, ecf_files, variables=None, triggers=None, def_status=None):
        """Construct the family.

        Args:
            name (str): Name of the family.
            parent (EcflowNodeContainer): Parent node.
            ecf_files (str): Location of ecf files
            variables (dict, optional): Variables to map. Defaults to None
            triggers (): Triggers. Defaults to None
            def_status (str, optional): Default status. Defaults to False.

        """
        EcflowNodeContainer.__init__(
            self, name, "family", parent, ecf_files, variables=variables, triggers=triggers,
            def_status=def_status
        )
        logging.debug(self.ecf_container_path)
        if self.ecf_node is not None:
            self.ecf_node.add_variable("ECF_FILES", self.ecf_container_path)


class EcflowSuiteTask(EcflowNode):
    """A task in an ecflow suite/family.

    Args:
        EcflowNode (EcflowNodeContainer): The node container.
    """

    def __init__(
        self,
        name,
        parent,
        config,
        task_settings,
        ecf_files,
        input_template=None,
        parse=True,
        variables=None,
        ecf_micro="%",
        triggers=None,
        def_status=None
    ):
        """Constuct the EcflowSuiteTask.

        Args:
            name (str): Name of task
            parent (EcflowNode): Parent node.
            ecf_files (str): Path to ecflow containers
            task_settings (TaskSettings): Submission configuration
            config (deode.ParsedConfig): Configuration file
            task_settings (deode.TaskSettings): Task settings
            input_template(str, optional): Input template
            parse (bool, optional): To parse template file or not
            variables (dict, optional): Variables to map. Defaults to None
            ecf_micro (str, optional): ECF_MICRO. Defaults to %
            triggers (): Triggers. Defaults to None
            def_status (str, optional): Default status. Defaults to False.

        Raises:
            Exception: Safety check
            FileNotFoundError: If the task container is not found.

        """
        EcflowNode.__init__(self, name, "task", parent, ecf_files, variables=variables,
                            triggers=triggers, def_status=def_status)

        logging.debug(parent.path)
        logging.debug(parent.ecf_container_path)
        task_container = parent.ecf_container_path + "/" + name + ".py"
        if parse:
            if input_template is None:
                raise Exception("Input template is missing")

            variables = task_settings.get_settings(name)
            if "INTERPRETER" in variables:
                interpreter = variables["INTERPRETER"]
            else:
                interpreter = f"{sys.executable}"
            logging.debug("vars %s", variables)
            for var, value in variables.items():
                logging.debug("value=%s", value)
                value = value.replace("@INTERPRETER@", interpreter.replace("#!", ""))
                value = value.replace("@NAME@", name)
                logging.debug("var=%s value=%s", var, value)
                if self.ecf_node is not None:
                    self.ecf_node.add_variable(var, value)
            task_settings.parse_job(
                name,
                config,
                input_template,
                task_container,
                variables=variables,
                ecf_micro=ecf_micro,
            )
        else:
            if not os.path.exists(task_container):
                raise FileNotFoundError(f"Container {task_container} is missing!")
