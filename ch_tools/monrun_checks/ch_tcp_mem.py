import os
from pathlib import Path

import click

from ch_tools.common.result import CRIT, OK, WARNING, Result

SOCKSTAT_PATH = Path("/proc/net/sockstat")

GIB = 1024**3
DEFAULT_CRIT_BYTES = 10 * GIB


@click.command("tcp-mem")
@click.option(
    "-c",
    "--critical",
    "crit",
    type=int,
    default=DEFAULT_CRIT_BYTES,
    show_default=True,
    help="Critical threshold in bytes.",
)
@click.option(
    "-w",
    "--warning",
    "warn",
    type=int,
    default=None,
    help="Warning threshold in bytes. Defaults to half of critical threshold.",
)
def tcp_mem_command(
    crit: int,
    warn: int,
) -> Result:
    """
    Check TCP memory usage from /proc/net/sockstat.
    """
    if warn is None:
        warn = crit // 2

    try:
        value = _get_tcp_mem_bytes()
    except Exception as e:
        return Result(CRIT, f"Failed to get TCP memory usage: {e}")

    if value > crit:
        return Result(CRIT, f"tcp_mem_bytes, crit = {crit}, count = {value}")

    if value > warn:
        return Result(WARNING, f"tcp_mem_bytes, warn = {warn}, count = {value}")

    return Result(OK)


def _get_tcp_mem_bytes() -> int:
    """
    Return current TCP memory usage from /proc/net/sockstat in bytes.
    """
    mem_pages = _get_tcp_mem_pages()
    page_size = os.sysconf("SC_PAGE_SIZE")

    return mem_pages * page_size


def _get_tcp_mem_pages() -> int:
    """
    Return current TCP memory usage from /proc/net/sockstat in memory pages.
    Example TCP line:
        TCP: inuse 37 orphan 0 tw 12 alloc 42 mem 8
    The "mem" value is reported in pages.
    """
    for line in SOCKSTAT_PATH.read_text(encoding="utf-8").splitlines():
        if not line.startswith("TCP:"):
            continue

        parts = line.split()

        try:
            mem_index = parts.index("mem")
            return int(parts[mem_index + 1])
        except ValueError:
            raise RuntimeError("TCP mem field is missing or invalid")
        except IndexError:
            raise RuntimeError("TCP mem value is missing")

    raise RuntimeError("TCP line not found in /proc/net/sockstat")
