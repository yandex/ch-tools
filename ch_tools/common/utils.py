import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Union

from click import Context
from file_read_backwards import FileReadBackwards

LOGS_REGEX_PREFIX = r"^([0-9]{4}\.[0-9]{2}\.[0-9]{2}\ [0-9]{2}\:[0-9]{2}\:[0-9]{2}).*?"


def escape_for_file_name(value: str) -> str:
    """Escape UTF-8 bytes using ClickHouse's ``escapeForFileName`` format."""
    result = []
    for byte in value.encode():
        char = chr(byte)
        if char.isascii() and (char.isalnum() or char == "_"):
            result.append(char)
        else:
            result.append(f"%{byte:02X}")
    return "".join(result)


def unescape_for_file_name(value: str) -> str:
    """Decode ClickHouse ``escapeForFileName`` sequences as UTF-8 bytes."""
    result = bytearray()
    index = 0
    while index < len(value):
        if value[index] == "%" and index + 2 < len(value):
            encoded_byte = value[index + 1 : index + 3]
            try:
                result.append(int(encoded_byte, 16))
            except ValueError:
                pass
            else:
                index += 3
                continue

        result.extend(value[index].encode())
        index += 1
    return result.decode()


def version_ge(version1: str, version2: str) -> bool:
    """
    Return True if version1 is greater or equal than version2.
    """
    return _parse_version(version1) >= _parse_version(version2)


def version_lt(version1: str, version2: str) -> bool:
    """
    Return True if version1 is less than version2.
    """
    return _parse_version(version1) < _parse_version(version2)


def _parse_version(version: str) -> list[int]:
    """
    Parse version string.
    """
    stripped_version = re.sub(r"^(\d+(?:\.\d+)*).*$", r"\1", version.strip())
    return [int(x) for x in stripped_version.split(".")]


def strip_query(query_text: str) -> str:
    """
    Remove query without newlines and duplicate whitespaces.
    Copy from ch-backup/ch-backup/util.py
    """
    return re.sub(r"\s{2,}", " ", query_text.replace("\n", " ")).strip()


def clear_empty_directories_recursively(directory: Union[str, Path]) -> None:
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


def execute(command: str) -> str:
    """
    Execute the specified command, check return code and return its output on success.
    """
    # pylint: disable=consider-using-with

    proc = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    stdout, stderr = proc.communicate()

    if proc.returncode:
        msg = f'"{command}" failed with code {proc.returncode}'
        if stderr:
            msg = f"{msg}: {stderr.decode()}"

        raise RuntimeError(msg)

    return stdout.decode()


def deep_merge(dest: Dict[Any, Any], update: Dict[Any, Any]) -> Dict[Any, Any]:
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


def first_key(mapping: Dict[Any, Any]) -> Any:
    return next(iter(mapping.keys()))


def first_value(mapping: Dict[Any, Any]) -> Any:
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


def get_by_key_path(object_: Any, key_path: str, default: Any = None) -> Any:
    """
    Get item by key path from object with arbitrary number of nested lists and dicts.
    """

    def _get_key(obj: Any, path: List[str]) -> Any:
        if not path:
            return default

        key = path.pop(0)

        if isinstance(obj, list):
            try:
                key = int(key)  # type: ignore
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


def update_by_key_path(object_: dict[str, Any], key_path: str, value: Any) -> None:
    """
    Update item by key path in object with arbitrary number of nested lists and dicts.
    """

    def _update(
        obj: dict[str, Any], path: List[str], value: Any, current_path_str: str
    ) -> None:
        if not path:
            return

        key = path.pop(0)
        current_path_str = (
            f'{current_path_str}."{key}"' if current_path_str else f'"{key}"'
        )

        if isinstance(obj, list):
            try:
                key = int(key)  # type: ignore
            except Exception:
                raise RuntimeError(
                    f'Key path "{key_path}" is invalid as {current_path_str} is a list.'
                )

        if not path:
            if isinstance(obj, list):
                list_size = len(obj)
                if key < list_size:  # type: ignore
                    obj[key] = value
                elif key == list_size:  # type: ignore
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


def count_log_messages(
    logfile: str, watch_seconds: int, match: str, exclude: str | None
) -> int:
    """
    Read logfile and count message occurrences.
    """
    datetime_start = datetime.now() - timedelta(seconds=watch_seconds)
    occurrences = 0
    match_regex = re.compile(LOGS_REGEX_PREFIX + match)

    exclude_regex = None
    if exclude is not None:
        exclude_regex = re.compile(LOGS_REGEX_PREFIX + exclude)

    with FileReadBackwards(logfile, encoding="utf-8") as f:
        for line in f:
            if exclude_regex is not None and exclude_regex.match(line):
                continue
            matched_line = match_regex.match(line)
            if matched_line is None:
                continue
            date = matched_line.group(1)
            if datetime.strptime(date, "%Y.%m.%d %H:%M:%S") < datetime_start:
                break
            occurrences += 1

    return occurrences
