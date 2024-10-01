"""
Logging module.
"""

import inspect
import logging
import sys
import traceback
from logging import (  # noqa # pylint:disable=unused-import
    CRITICAL,
    DEBUG,
    ERROR,
    FATAL,
    INFO,
    WARN,
    WARNING,
)
from typing import Any, Dict, Optional

from loguru import logger

from ch_tools.common import result

logger_config: Dict[str, Any] = {}


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


def configure(
    config_loguru: dict,
    module: str,
    extra: Optional[dict] = None,
) -> None:
    """
    Configure logger.
    """
    logger_config.clear()
    logger_config["module"] = module
    loguru_handlers = []

    for name, value in config_loguru["handlers"].get(module, {}).items():
        format_ = config_loguru["formatters"][value["format"]]
        handler = {
            "sink": value["sink"],
            "format": format_,
            "enqueue": True,
            "diagnose": False,
            "backtrace": False,
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

    logger.configure(handlers=loguru_handlers, activation=[("", True)], extra=extra)

    enable_stdout_logger()

    # Configure logging.
    logging.basicConfig(handlers=[InterceptHandler()], level=0)


def _log(level, msg, *args, **kwargs):
    exc_info = kwargs.get("exc_info", False)
    getLogger(logger_config["module"]).opt(exception=exc_info).log(
        level, msg, *args, **kwargs
    )


def critical(msg, *args, **kwargs):
    """
    Log a message with severity 'CRITICAL'.
    """
    _log("CRITICAL", msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    _log("ERROR", msg, *args, **kwargs)


def exception(msg, *args, exc_info=True, short_stdout=False, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """
    if not short_stdout:
        _log("ERROR", msg, *args, exc_info=exc_info, **kwargs)
        return

    print_last_exception()
    if logger_config.get("stdout_logger_id", None):
        disable_stdout_logger()
        _log("ERROR", msg, *args, exc_info=exc_info, **kwargs)
        enable_stdout_logger()
    else:
        _log("ERROR", msg, *args, exc_info=exc_info, **kwargs)


def warning(msg, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    _log("WARNING", msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    _log("INFO", msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    _log("DEBUG", msg, *args, **kwargs)


def log_status(status, msg):
    """
    Log a message by status code.
    """
    if status == result.OK:
        debug(msg)
    elif status == result.WARNING:
        warning(msg)
    else:
        error(msg)


# pylint: disable=invalid-name
def getLogger(name: str) -> Any:
    """
    Get logger with specific name.
    """
    return logger.bind(logger_name=name)


# pylint: disable=invalid-name
def getNativeLogger(name: str) -> Any:
    """
    Get logging logger with specific name. Might be used for interaction with other libraries.
    """
    return logging.getLogger(name)


def add(sink, level, format_):
    """
    Add new log handler.
    """
    logger.add(sink=sink, level=level, format=format_)


def set_module_log_level(module, level):
    """
    Set level for logging's logger. Might be used to control logs from other libraries.
    """
    getNativeLogger(module).setLevel(level)


def disable_stdout_logger():
    """
    Removes stdout handler. May be used for commands with "quiet" option.
    """
    if logger_config["stdout_logger_id"]:
        logger.remove(logger_config["stdout_logger_id"])
        logger_config["stdout_logger_id"] = None


def enable_stdout_logger():
    """
    Adds stdout logger.
    """
    if logger_config.get("stdout_logger_id", None) is None:
        logger_config["stdout_logger_id"] = logger.add(
            sink=sys.stdout,
            level="INFO",
            format="{message}",
            filter=make_filter(logger_config["module"]),
            backtrace=False,
            diagnose=False,
        )


def print_last_exception():
    exc_type, value, traceback_ = sys.exc_info()
    traceback.print_exception(exc_type, value, traceback_, chain=False)
