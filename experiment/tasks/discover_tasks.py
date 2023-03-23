"""Discover tasks."""
import importlib
import inspect
import os
import pkgutil
import sys

from .. import PACKAGE_NAME, tasks
from ..logs import get_logger
from .tasks import AbstractTask


def discover_modules(package, what="plugin", loglevel="INFO"):
    """Discover plugin modules.

    Args:
        package (types.ModuleType): Namespace package containing the plugins
        what (str, optional): String describing what is supposed to be discovered.
                              Defaults to "plugin".
        loglevel(str, optional): Loglevel. Default to "INFO"

    Yields:
        str (types.ModuleType):  Imported module

    """
    path = package.__path__
    prefix = package.__name__ + "."

    logger = get_logger(PACKAGE_NAME, loglevel=loglevel)
    logger.debug("%s search path: %r", what.capitalize(), path)
    for _finder, mname, _ispkg in pkgutil.iter_modules(path):
        fullname = prefix + mname
        logger.debug("Loading module %r", fullname)
        try:
            mod = importlib.import_module(fullname)
        except RuntimeError as exc:
            logger.warning("Could not load %r %s", fullname, repr(exc))
            continue
        yield fullname, mod


def _get_name(cname, cls, suffix, attrname="__plugin_name__"):
    """Get name.

    Args:
        cname (_type_): cname
        cls (_type_): cls
        suffix (str): suffix
        attrname (str, optional): _description_. Defaults to "__plugin_name__".

    Returns:
        _type_: Name

    """
    # __dict__ vs. getattr: do not inherit the attribute from a parent class
    name = getattr(cls, "__dict__", {}).get(attrname, None)
    if name is not None:
        return name
    name = cname.lower()
    if name.endswith(suffix):
        name = name[: -len(suffix)]
    return name


def get_task(name, config, loglevel="INFO"):
    """Create a `AbstractTask` object from configuration.

    Args:
        name (str): _description_
        config (ParsedConfig): _description_
        loglevel(str, optional): Loglevel. Default to "INFO"

    Returns:
        AbstractTask: The task object

    """
    logger = get_logger(PACKAGE_NAME, loglevel=loglevel)
    task_name = name.lower()
    plugin_namespace = None
    plugin_namespace_location = (
        f"{config.get_value('system.exp_dir')}/experiment_plugin_tasks"
    )
    if os.path.exists(plugin_namespace_location):
        logger.info("Using local plugin directory %s", plugin_namespace_location)
        sys.path.insert(0, config.exp_dir)
        import experiment_plugin_tasks as plugin_namespace  # noqa

    known_types = discover(tasks, AbstractTask, attrname="__type_name__")
    logger.debug("Available task types: %s", ", ".join(known_types.keys()))
    plugin_known_types = {}
    if plugin_namespace is not None:
        plugin_known_types = discover(
            plugin_namespace, AbstractTask, attrname="__type_name__"
        )
        logger.debug(
            "Available plugin task types: %s", ", ".join(plugin_known_types.keys())
        )

    if task_name in plugin_known_types:
        cls = plugin_known_types[task_name]
    else:
        cls = known_types[task_name]
    task = cls(config)
    logger.debug("Created %r for %s", task, name)
    return task


def discover(package, base, attrname="__plugin_name__", loglevel="INFO"):
    """Discover task classes.

    Plugin classes are discovered in a given namespace package, deriving from
    a given base class. The base class itself is ignored, as are classes
    imported from another module (based on ``cls.__module__``). Each discovered
    class is identified by a name that is either the value of attribute
    ``attrname`` if present, or deduced from the class name by changing it to
    lowercase and stripping the name of the base class, if it appears as a
    suffix.

    Args:
        package (types.ModuleType): Namespace package containing the plugins
        base (type): Base class for the plugins
        attrname (str): Name of the attribute that contains the name for the plugin
        loglevel(str, optional): Loglevel. Default to "INFO"

    Returns:
        (dict of str: type): Discovered plugin classes

    """
    logger = get_logger(PACKAGE_NAME, loglevel=loglevel)
    what = base.__name__

    def pred(x):
        return inspect.isclass(x) and issubclass(x, base) and x is not base

    discovered = {}
    for fullname, mod in discover_modules(package, what=what):
        for cname, cls in inspect.getmembers(mod, pred):
            tname = _get_name(cname, cls, what.lower(), attrname=attrname)
            if cls.__module__ != fullname:
                logger.debug(
                    "Skipping %s %r imported by %r", what.lower(), tname, fullname
                )
                continue
            if tname in discovered:
                logger.warning(
                    "%s type %r is defined more than once", what.capitalize(), tname
                )
                continue
            discovered[tname] = cls
    return discovered
