"""A set of tools for administration and diagnostics of ClickHouse DBMS."""

from pkg_resources import resource_string

__version__ = resource_string(__name__, "version.txt").decode().strip()
