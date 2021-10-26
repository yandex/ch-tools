import re
import os
from pathlib import Path


def version_ge(version1, version2):
    """
    Return True if version1 is greater or equal than version2.
    """
    return parse_version(version1) >= parse_version(version2)


def parse_version(version):
    """
    Parse version string.
    """
    return [int(x) for x in version.strip().split('.')]


def strip_query(query_text: str) -> str:
    """
    Remove query without newlines and duplicate whitespaces.
    Copy from ch-backup/ch-backup/util.py
    """
    return re.sub(r'\s{2,}', ' ', query_text.replace('\n', ' ')).strip()


def clear_empty_directories_recursively(directory):
    directory = Path(directory)
    for item in directory.iterdir():
        if item.is_dir():
            clear_empty_directories_recursively(item)
    if len(os.listdir(directory)) == 0:
        directory.rmdir()
