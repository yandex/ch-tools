"""
Utility functions.
"""

from collections import defaultdict
from datetime import datetime, timedelta

import humanfriendly
from dateutil.tz import gettz
from deepdiff import DeepDiff


def parse_timespan(value):
    """
    Parse time span value.
    """
    return timedelta(seconds=humanfriendly.parse_timespan(value))


def now(ctx):
    """
    Like `datetime.now`, but with timezone information.
    """
    return datetime.now(get_timezone(ctx))


def get_timezone(ctx):
    if "timezone" not in ctx.obj:
        config = ctx.obj["config"]
        ctx.obj["timezone"] = gettz(config.get("timezone", "UTC"))

    return ctx.obj["timezone"]


def diff_objects(value1, value2):
    """
    Calculate structural diff between 2 values.
    """
    ignore_type_in_groups = [(dict, defaultdict)]
    return DeepDiff(
        value1,
        value2,
        verbose_level=2,
        view="tree",
        ignore_type_in_groups=ignore_type_in_groups,
    )


class Nullable:
    """
    Nullable wrapper type. It helps to distinguish the cases when a value is not specified vs.
    it's specified None value.
    """

    def __init__(self, value=None):
        self.value = value


def flatten_nullable(value):
    if value is None:
        return False, None

    if isinstance(value, Nullable):
        value = value.value

    return True, value


def is_not_null(value):
    """
    Return True if the value is not null.
    """
    if isinstance(value, Nullable):
        value = value.value

    return value is not None
