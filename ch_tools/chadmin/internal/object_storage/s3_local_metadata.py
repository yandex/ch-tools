import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

from typing_extensions import Self

MAX_METADATA_FILE_SIZE = 10 * 1024


@dataclass
class S3ObjectLocalInfo:
    """
    Information about the S3 object stored locally in the metadata file.
    """

    key: str
    size: int


@dataclass
class S3ObjectLocalMetaData:
    """
    Parsed content of metadata file stored on the local disk.
    """

    version: int
    total_size: int
    objects: List[S3ObjectLocalInfo]
    ref_counter: int
    read_only: bool

    @classmethod
    def from_string(cls, value: str) -> Self:
        lines = value.splitlines()
        idx = 0

        matches = re.match(r"^[123]$", lines[idx])
        if not matches:
            raise ValueError(f"Incorrect metadata version. Line: `{lines[idx]}`")
        version = int(matches[0])
        idx += 1

        matches = re.match(r"^(\d+)\s+(\d+)$", lines[idx])
        if not matches:
            raise ValueError(
                f"Incorrect metadata about the objects count and total size. Line: `{lines[idx]}`"
            )
        object_count, total_size = int(matches[1]), int(matches[2])
        idx += 1

        objects: List[S3ObjectLocalInfo] = []
        for _ in range(object_count):
            matches = re.match(r"^(\d+)\s+(\S+)$", lines[idx])
            if not matches:
                raise ValueError(
                    f"Incorrect metadata about object size and name. Line: `{lines[idx]}`"
                )
            objects.append(S3ObjectLocalInfo(key=matches[2], size=int(matches[1])))
            idx += 1

        matches = re.match(r"^\d+$", lines[idx])
        if not matches:
            raise ValueError(
                f"Incorrect metadata about refcounter. Line: `{lines[idx]}`"
            )
        refcounter = int(lines[idx])
        idx += 1

        matches = re.match("^[01]$", lines[idx])
        if not matches:
            raise ValueError(
                f"Incorrect metadata about readonly flag. Line: `{lines[idx]}`"
            )
        read_only = bool(int(matches[0]))

        return cls(
            version=version,
            total_size=total_size,
            objects=objects,
            ref_counter=refcounter,
            read_only=read_only,
        )

    @classmethod
    def from_file(cls, path: Path) -> Self:
        if path.stat().st_size > MAX_METADATA_FILE_SIZE:
            raise ValueError(
                f"Metadata file too large. Its size must not exceed {MAX_METADATA_FILE_SIZE} bytes"
            )
        with path.open(encoding="latin-1") as file:
            return cls.from_string(file.read())
