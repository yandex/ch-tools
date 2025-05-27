"""A set of tools for administration and diagnostics of ClickHouse DBMS."""

import warnings

from pkg_resources import resource_string

warnings.filterwarnings(
    "ignore", category=UserWarning, message="pkg_resources is deprecated"
)


__version__ = resource_string(__name__, "version.txt").decode().strip()
