"""A set of tools for administration and diagnostics of ClickHouse DBMS."""

from importlib.resources import files

__version__ = files(__name__).joinpath("version.txt").read_text().strip()
