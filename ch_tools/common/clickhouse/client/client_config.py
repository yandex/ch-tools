"""Keep client credentials in a private configuration instead of argv or env."""

import hashlib
import os
import re
import subprocess
from contextlib import contextmanager
from pathlib import Path
from shutil import which
from tempfile import TemporaryDirectory, TemporaryFile
from typing import Dict, Iterator, List, Optional, Tuple
from uuid import uuid4
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from ch_tools.common.utils import version_ge

_CLIENT_VERSIONS: Dict[tuple, str] = {}


def _client_version(env: Dict[str, str]) -> str:
    """Cache versions by executable metadata, cwd and a secret-free environment digest."""
    executable = which("clickhouse-client", path=env.get("PATH"))
    if executable is None:
        raise FileNotFoundError("clickhouse-client executable not found")
    info = os.stat(executable)
    fingerprint = hashlib.sha256(
        repr(
            sorted(
                (key, value)
                for key, value in env.items()
                if key != "CLICKHOUSE_PASSWORD"
            )
        ).encode()
    ).digest()
    key = (
        str(Path(executable).resolve()),
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
        os.getcwd(),
        fingerprint,
    )
    version = _CLIENT_VERSIONS.get(key)
    if version is None:
        result = subprocess.run(
            [executable, "--version"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        match = re.search(rb"\b(\d+\.\d+\.\d+(?:\.\d+)?)\b", result.stdout)
        if match is None:
            raise RuntimeError("Cannot determine clickhouse-client version")
        if len(_CLIENT_VERSIONS) >= 8:
            _CLIENT_VERSIONS.clear()
        version = match[1].decode()
        _CLIENT_VERSIONS[key] = version
    return version


def _config_path(env: Dict[str, str]) -> Optional[Path]:
    """Find the default config using the installed client's search order.

    YAML discovery appeared in 23.8; XDG paths were added in 25.12.
    Native discovery skips stat errors, including inaccessible HOME after setuid.
    """
    version = _client_version(env)
    names = [Path("clickhouse-client")]
    home = env.get("HOME")
    if version_ge(version, "25.12"):
        xdg = env.get("XDG_CONFIG_HOME")
        if xdg is not None or home is not None:
            names.append(
                (Path(xdg) if xdg is not None else Path(home or "") / ".config")
                / "clickhouse/config"
            )
    if home:
        names.append(Path(home) / ".clickhouse-client/config")
    names.append(Path("/etc/clickhouse-client/config"))
    extensions = (".xml", ".yaml", ".yml") if version_ge(version, "23.8") else (".xml",)
    return next(
        (
            path.absolute()
            for name in names
            for ext in extensions
            if os.path.exists(path := name.with_suffix(ext))
        ),
        None,
    )


def _mirror_config(source: Optional[Path], directory: Path) -> Tuple[Path, str]:
    """Mirror config names and leave fragment selection and ordering to ClickHouse."""
    destination = directory / (source.name if source else "config.xml")
    root = "clickhouse"
    if source:
        destination.symlink_to(source)
        if source.suffix == ".xml":
            with source.open("rb") as stream:
                root = next(ElementTree.iterparse(stream, events=("start",)))[1].tag
        for merge_dir in (source.with_suffix(".d"), source.parent / "conf.d"):
            if merge_dir.is_dir():
                target = directory / merge_dir.name
                target.mkdir(mode=0o700)
                for entry in merge_dir.iterdir():
                    (target / entry.name).symlink_to(entry)
    else:
        destination.write_text("<clickhouse/>", encoding="utf-8")
    destination.with_suffix(".d").mkdir(mode=0o700, exist_ok=True)
    return destination, root


def _password_xml(key: str, password: str) -> str:
    """Encode a password without XML injection or newline normalization."""
    if password == "\n" or any(
        not (
            char in "\t\n\r"
            or "\x20" <= char <= "\ud7ff"
            or "\ue000" <= char <= "\ufffd"
            or "\U00010000" <= char <= "\U0010ffff"
        )
        for char in password
    ):
        raise ValueError("TCP client password contains unsupported characters")
    value = escape(password).replace("\r", "&#13;")
    if "$" in password:
        value = value.replace("$", "${" + key + "_dollar}")
        return f"<{key}>{value}</{key}><{key}_dollar>$</{key}_dollar>"
    return f"<{key}>{value}</{key}>"


@contextmanager
def client_config(
    password: Optional[str],
) -> Iterator[Tuple[List[str], Dict[str, str], Tuple[int, ...]]]:
    """Supply credentials through private config properties, preserving native precedence.

    Poco expands property references when the client reads a password. Keeping
    separate references for argv and env preserves version-specific precedence,
    including connections_credentials and from_env substitutions.
    """
    env = os.environ.copy()
    args: List[str] = []
    properties = []
    password_key = f"ch_tools_{uuid4().hex}"
    env_key = f"{password_key}_env"
    if password is not None:
        properties.append(_password_xml(password_key, password))
        args.extend(("--password", "${" + password_key + "}"))
    if "CLICKHOUSE_PASSWORD" in env:
        properties.append(_password_xml(env_key, env["CLICKHOUSE_PASSWORD"]))
        env["CLICKHOUSE_PASSWORD"] = "${" + env_key + "}"
    if not properties:
        yield args, env, ()
        return

    source = _config_path(env)
    with TemporaryDirectory(prefix="ch-tools-client-") as temporary:
        directory = Path(temporary)
        config, root = _mirror_config(source, directory)
        with TemporaryFile() as credentials:
            credentials.write(
                (f"<{root}>" + "".join(properties) + f"</{root}>").encode()
            )
            credentials.seek(0)
            descriptor = credentials.fileno()
            (config.with_suffix(".d") / f"{password_key}.xml").symlink_to(
                f"/dev/fd/{descriptor}"
            )
            yield ["--config-file", str(config), *args], env, (descriptor,)
