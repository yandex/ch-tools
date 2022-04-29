"""
Utility functions.
"""

import logging
import string
from functools import wraps
from random import choice as random_choise
from types import SimpleNamespace
from typing import Mapping, MutableMapping

from .typing import ContextT


def merge(original, update):
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


def env_stage(event, fail=False):
    """
    Nicely logs env stage.
    """

    def wrapper(fun):
        @wraps(fun)
        def _wrapped_fun(*args, **kwargs):
            stage_name = f'{fun.__module__}.{fun.__name__}'
            logging.info('initiating %s stage %s', event, stage_name)
            try:
                return fun(*args, **kwargs)
            except Exception as e:
                logging.error('%s failed: %s', stage_name, e)
                if fail:
                    raise

        return _wrapped_fun

    return wrapper


def generate_random_string(length: int = 64) -> str:
    """
    Generate random alphanum sequence.
    """
    return ''.join(random_choise(string.ascii_letters + string.digits) for _ in range(length))


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
