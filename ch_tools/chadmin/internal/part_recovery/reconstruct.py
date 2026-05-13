"""
Reconstruction helpers for missing or damaged part files.

Provides:
- :func:`count_rows_from_mrk2` — derive row count from a ``.mrk2`` file.
- :func:`zero_fill` — create a zero-filled stub file of a given size.
- :func:`default_compression_codec_txt` — stub content for ``default_compression_codec.txt``.
- :func:`metadata_version_txt` — stub content for ``metadata_version.txt``.
- :func:`write_count_txt` — write ``count.txt`` with a given row count.
"""

import struct
from pathlib import Path

# .mrk2 record layout: three UInt64 values (little-endian, 8 bytes each)
#   offset_in_compressed_file  (8 bytes)
#   offset_in_decompressed_block (8 bytes)
#   number_of_rows_in_granule   (8 bytes)
_MRK2_RECORD_SIZE = 24
_MRK2_STRUCT = struct.Struct("<QQQ")


def count_rows_from_mrk2(path: Path) -> int:
    """
    Read a ``.mrk2`` file and return the total number of rows it covers.

    Each record in a ``.mrk2`` file is 24 bytes:
    ``(offset_in_compressed, offset_in_decompressed, granule_rows)``.
    The sum of all ``granule_rows`` values equals the total row count of the part.

    Raises :class:`ValueError` if the file size is not a multiple of 24.
    """
    size = path.stat().st_size
    if size % _MRK2_RECORD_SIZE != 0:
        raise ValueError(
            f"{path} has size {size} which is not a multiple of {_MRK2_RECORD_SIZE}. "
            "The file may be corrupted."
        )

    total_rows = 0
    data = path.read_bytes()
    for offset in range(0, size, _MRK2_RECORD_SIZE):
        _off_compressed, _off_decompressed, granule_rows = _MRK2_STRUCT.unpack_from(
            data, offset
        )
        total_rows += granule_rows

    return total_rows


def zero_fill(path: Path, size: int) -> None:
    """
    Create (or overwrite) *path* with exactly *size* zero bytes.

    This is used to create stub files for missing index/meta files so that
    ClickHouse can still ATTACH the part (with ``force_restore_data=1``).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        fh.write(b"\x00" * size)


def default_compression_codec_txt() -> bytes:
    """
    Return the stub content for ``default_compression_codec.txt``.

    ClickHouse writes the codec name used for the part's columns.  When the
    file is missing we fall back to LZ4 which is the default.
    """
    return b"CODEC(LZ4)\n"


def metadata_version_txt() -> bytes:
    """
    Return the stub content for ``metadata_version.txt``.

    Version 0 is the baseline; ClickHouse will accept it during ATTACH.
    """
    return b"0\n"


def write_count_txt(path: Path, row_count: int) -> None:
    """Write *row_count* to ``count.txt`` at *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(row_count) + "\n", encoding="utf-8")


def reconstruct_missing_meta(
    dest_dir: Path,
    name: str,
    size: int,
) -> None:
    """
    Write a stub for a missing meta file into *dest_dir*.

    Handles ``default_compression_codec.txt``, ``metadata_version.txt``,
    and any other meta file that should be zero-filled.
    """
    dest = dest_dir / name
    if name == "default_compression_codec.txt":
        dest.write_bytes(default_compression_codec_txt())
    elif name == "metadata_version.txt":
        dest.write_bytes(metadata_version_txt())
    else:
        zero_fill(dest, size)
