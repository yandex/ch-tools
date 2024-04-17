"""
Logging module.
"""

import inspect
import logging
from typing import Any, Dict, Optional

from loguru import logger

this: Dict[str, Any] = {}


class Filter:
    """
    Filter for luguru handler.
    """

    def __init__(self, name):
        self._name = name

    def __call__(self, record):
        """
        Filter callback to decide for each logged message whether it should be sent to the sink or not.
        """

        return record["extra"].get("logger_name") == self._name


def make_filter(name):
    """
    Factory for filter creation.
    """

    return Filter(name)


class InterceptHandler(logging.Handler):
    """
    Helper class for logging interception.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """
        Intercept all records from the logging module and redirect them into loguru.

        The handler for loguru will be chosen based on module name.
        """

        # Get corresponding Loguru level if it exists.
        level: int or str  # type: ignore
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.bind(logger_name=record.name).opt(
            depth=depth, exception=record.exc_info
        ).log(level, record.getMessage())


def configure(config_loguru: dict, module: str, cmd_name: Optional[str] = None) -> None:
    """
    Configure logger.
    """
    this["module"] = module
    this["cmd_name"] = cmd_name
    loguru_handlers = []

    for name, value in config_loguru["handlers"].get(module, {}).items():
        format_ = config_loguru["formatters"][value["format"]]
        handler = {
            "sink": value["sink"],
            "format": format_,
            "enqueue": True,
            "diagnose": False,
        }
        if "level" in value:
            handler["level"] = value["level"]

        if "filter" in value:
            if value["filter"]:
                handler["filter"] = make_filter(value["filter"])
                # https://loguru.readthedocs.io/en/stable/api/logger.html#message
                # One can also pass a dict mapping module names to minimum required level. In such case, each log record will search for it’s closest parent in the dict
                # and use the associated level as the filter.
                # In order to set a default level, the "" module name should be used as it is the parent of all modules (it does not suppress global level threshold, though).
                handler["filter"][""] = False
        else:
            handler["filter"] = make_filter(name)
        loguru_handlers.append(handler)

    logger.configure(handlers=loguru_handlers, activation=[("", True)])

    # Configure logging.
    logging.basicConfig(handlers=[InterceptHandler()], level=0)


def critical(msg, *args, **kwargs):
    """
    Log a message with severity 'CRITICAL'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger(this["module"]).opt(exception=with_exception).critical(
        msg, *args, **kwargs
    )


def error(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger(this["module"]).opt(exception=with_exception).error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """

    with_exception = kwargs.get("exc_info", False)
    getLogger(this["module"]).opt(exception=with_exception).debug(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger(this["module"]).opt(exception=with_exception).warning(
        msg, *args, **kwargs
    )


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger(this["module"]).opt(exception=with_exception).info(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger(this["module"]).opt(exception=with_exception).debug(msg, *args, **kwargs)


def log_status(status, msg):
    """
    Log a message with by status code.
    """
    if status == 0:
        debug(msg)
    elif status == 1:
        warning(msg)
    else:
        error(msg)


# pylint: disable=invalid-name
def getLogger(name: str) -> Any:
    """
    Get logger with specific name.
    """
    return logger.bind(logger_name=name, cmd_name=this.get("cmd_name", ""))


def add(sink, level, format_):
    """
    Add new log handler.
    """
    logger.add(sink=sink, level=level, format=format_)


def set_module_log_level(module, level):
    """
    Set level for logging's logger. Might be used to control logs from other libraries.
    """
    logging.getLogger(module).setLevel(level)
