"""
Utility functions.
"""

import string
from functools import wraps
from random import choice as random_choise
from types import SimpleNamespace
from typing import Any, Callable, Mapping, MutableMapping

from ch_tools.common import logging, utils

from .typing import ContextT


def merge(
    original: MutableMapping[Any, Any], update: Mapping[Any, Any]
) -> MutableMapping[Any, Any]:
    """
    Recursively merge update dict into original.
    """
    for key in update:
        recurse_conditions = [
            key in original,
            isinstance(original.get(key), MutableMapping),
            isinstance(update.get(key), Mapping),
        ]
        if all(recurse_conditions):
            merge(original[key], update[key])
        else:
            original[key] = update[key]
    return original


def env_stage(event: str, fail: bool = False) -> Callable:
    """
    Nicely logs env stage.
    """

    def wrapper(fun: Callable) -> Callable:
        @wraps(fun)
        def _wrapped_fun(*args: Any, **kwargs: Any) -> Any:
            stage_name = f"{fun.__module__}.{fun.__name__}"
            logging.info("initiating {} stage {}", event, stage_name)
            try:
                return fun(*args, **kwargs)
            except Exception as e:
                logging.error("{} failed: {!r}", stage_name, e)
                if fail:
                    raise

        return _wrapped_fun

    return wrapper


def generate_random_string(length: int = 64) -> str:
    """
    Generate random alphanum sequence.
    """
    return "".join(
        random_choise(string.ascii_letters + string.digits) for _ in range(length)
    )


def context_to_dict(context: ContextT) -> dict:
    """
    Convert context to dict representation.

    The context type can be either types.SimpleNamespace or behave.Context.
    """
    if isinstance(context, SimpleNamespace):
        return context.__dict__

    result: dict = {}
    for frame in context._stack:  # pylint: disable=protected-access
        for key, value in frame.items():
            if key not in result:
                result[key] = value

    return result


def version_ge(current_version: str, comparing_version: str) -> bool:
    """
    Return True if `current_version` is greater or equal than `comparing_version`, or False otherwise.
    """
    # "latest" is greater or equal than any known version
    if current_version == "latest":
        return True

    return utils.version_ge(current_version, comparing_version)


def version_lt(current_version: str, comparing_version: str) -> bool:
    """
    Return True if `current_version` is less than `comparing_version`, or False otherwise.
    """
    # "latest" is not less than any known version
    if current_version == "latest":
        return False

    return utils.version_lt(current_version, comparing_version)
