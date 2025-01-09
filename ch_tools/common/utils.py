import os
import re
import subprocess
from pathlib import Path

from click import Context


def version_ge(version1, version2):
    """
    Return True if version1 is greater or equal than version2.
    """
    return parse_version(version1) >= parse_version(version2)


def parse_version(version):
    """
    Parse version string.
    """
    return [int(x) for x in version.strip().split(".")]


def strip_query(query_text: str) -> str:
    """
    Remove query without newlines and duplicate whitespaces.
    Copy from ch-backup/ch-backup/util.py
    """
    return re.sub(r"\s{2,}", " ", query_text.replace("\n", " ")).strip()


def clear_empty_directories_recursively(directory):
    try:
        directory = Path(directory)
        for item in directory.iterdir():
            if item.is_dir():
                clear_empty_directories_recursively(item)
        if len(os.listdir(directory)) == 0:
            directory.rmdir()
    except FileNotFoundError:
        print(
            f"Tried to remove directory {directory}, but the error arose. Maybe it was already removed."
        )


def execute(command):
    """
    Execute the specified command, check return code and return its output on success.
    """
    # pylint: disable=consider-using-with

    proc = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    stdout, stderr = proc.communicate()

    if proc.returncode:
        msg = '"{0}" failed with code {1}'.format(command, proc.returncode)
        if stderr:
            msg = "{0}: {1}".format(msg, stderr.decode())

        raise RuntimeError(msg)

    return stdout.decode()


def deep_merge(dest, update):
    """
    Deep merge two dictionaries.
    Like `dict.update`, but instead of updating only top-level keys, perform recursive dict merge.
    """
    for key, value in update.items():
        if key in dest and isinstance(dest[key], dict) and isinstance(value, dict):
            deep_merge(dest[key], value)
        else:
            dest[key] = value
    return dest


def first_key(mapping):
    return next(iter(mapping.keys()))


def first_value(mapping):
    return next(iter(mapping.values()))


def get_full_command_name(ctx: Context) -> str:
    """
    Return full command name (with names of groups).
    """
    if ctx.parent is None:
        return ""

    cmd_name = ctx.command.name or "unknown"
    parent_cmd_name = get_full_command_name(ctx.parent)
    return f"{parent_cmd_name} {cmd_name}" if parent_cmd_name else cmd_name


def get_by_key_path(object_, key_path, default=None):
    """
    Get item by key path from object with arbitrary number of nested lists and dicts.
    """

    def _get_key(obj, path):
        key = path.pop(0)

        if isinstance(obj, list):
            try:
                key = int(key)
            except Exception:
                return default

        if not path:
            try:
                return obj[key]
            except Exception:
                return default
        else:
            try:
                return _get_key(obj[key], path)
            except Exception:
                return default

    return _get_key(object_, key_path.split("."))


def update_by_key_path(object_, key_path, value):
    """
    Update item by key path in object with arbitrary number of nested lists and dicts.
    """

    def _update(obj, path, value, current_path_str):
        key = path.pop(0)
        current_path_str = (
            f'{current_path_str}."{key}"' if current_path_str else f'"{key}"'
        )

        if isinstance(obj, list):
            try:
                key = int(key)
            except Exception:
                raise RuntimeError(
                    f'Key path "{key_path}" is invalid as {current_path_str} is a list.'
                )

        if not path:
            if isinstance(obj, list):
                list_size = len(obj)
                if key < list_size:
                    obj[key] = value
                elif key == list_size:
                    obj.append(value)
                else:
                    raise RuntimeError(
                        f'Key path "{key_path}" is invalid as {current_path_str} is a list with {list_size} elements.'
                    )
            else:
                obj[key] = value
        else:
            if isinstance(obj, list):
                list_size = len(obj)
                if key < len(obj):
                    _update(obj[key], path, value, current_path_str)
                else:
                    raise RuntimeError(
                        f'Key path "{key_path}" is invalid as {current_path_str} is a list with {list_size} elements.'
                    )
            else:
                if obj.get(key) is None:
                    obj[key] = {}
                _update(obj[key], path, value, current_path_str)

    _update(object_, key_path.split("."), value, "")
