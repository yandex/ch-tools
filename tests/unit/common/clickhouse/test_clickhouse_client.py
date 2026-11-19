import hashlib
import json
import os
import select
import signal
import subprocess
import sys
import traceback
from pathlib import Path
from textwrap import dedent
from typing import Optional

import pytest

from ch_tools.common.clickhouse.client import client_config as config_module
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient
from ch_tools.common.clickhouse.client.client_config import client_config
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort


@pytest.fixture(name="client_process")
def fixture_client_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Inspect the actual subprocess channels without emulating ClickHouse auth."""
    executable = tmp_path / "clickhouse-client"
    executable.write_text(
        f"#!{sys.executable}\n"
        + dedent(
            """\
            import hashlib, json, os, signal, stat, sys
            from pathlib import Path
            from xml.etree import ElementTree

            if '--version' in sys.argv:
                print('ClickHouse client version ' + os.environ.get('TEST_CLIENT_VERSION', '25.8.23.13'))
                sys.exit(0)
            properties = {}
            config_path = None
            secret_mode = None
            directory_mode = None
            if '--config-file' in sys.argv:
                config_path = Path(sys.argv[sys.argv.index('--config-file') + 1])
                credentials = next(path for path in config_path.with_suffix('.d').iterdir() if path.is_symlink() and os.readlink(path).startswith('/dev/fd/'))
                properties = {node.tag: node.text or '' for node in ElementTree.parse(credentials).getroot()}
                secret_mode = stat.S_IMODE(credentials.stat().st_mode)
                directory_mode = stat.S_IMODE(config_path.parent.stat().st_mode)
            query = sys.stdin.buffer.read().decode()
            report = {
                'pid': os.getpid(),
                'argv': sys.argv[1:],
                'hashes': {key: hashlib.sha256(value.encode()).hexdigest() for key, value in properties.items()},
                'query': query,
                'env_password': os.environ.get('CLICKHOUSE_PASSWORD'),
                'same_process_group': os.getpgrp() == os.getpgid(os.getppid()),
                'config_path': str(config_path) if config_path else None,
                'secret_mode': secret_mode,
                'directory_mode': directory_mode,
            }
            Path(os.environ['TEST_CLIENT_REPORT']).write_text(json.dumps(report))
            if os.environ.get('TEST_CLIENT_WAIT'):
                def stop(signum, _frame):
                    "Record cancellation before exiting."
                    signal.signal(signal.SIGINT, signal.SIG_IGN)
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                    Path(os.environ['TEST_CLIENT_STOPPED']).write_text(str(signum))
                    sys.exit(0)
                signal.signal(signal.SIGINT, stop)
                signal.signal(signal.SIGTERM, stop)
                with open(os.environ['TEST_CLIENT_READY'], 'w') as ready:
                    ready.write('ready\\n')
                signal.pause()
            if os.environ.get('TEST_CLIENT_ERROR'):
                sys.stderr.write('Native diagnostic: ' + ' '.join(properties.values()))
                sys.exit(42)
            print(json.dumps(report))
            """
        ),
        encoding="utf-8",
    )
    executable.chmod(0o700)
    (tmp_path / "clickhouse-client.xml").write_text("<config/>", encoding="utf-8")
    report = tmp_path / "report.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("TEST_CLIENT_REPORT", str(report))
    for name in (
        "CLICKHOUSE_PASSWORD",
        "TEST_CLIENT_ERROR",
        "TEST_CLIENT_VERSION",
        "XDG_CONFIG_HOME",
    ):
        monkeypatch.delenv(name, raising=False)
    return report


def make_client(password: Optional[str]) -> ClickhouseClient:
    """Create a client with both native transports available."""
    return ClickhouseClient(
        host="localhost",
        user="test-user",
        password=password,
        ports={ClickhousePort.TCP: 9000, ClickhousePort.TCP_SECURE: 9440},
        timeout=5,
    )


@pytest.mark.parametrize(
    "password,inherited",
    [
        (None, None),
        ("", None),
        (" secret ' \\\" & < > пароль $ ", None),
        ("x" * 1500, None),
        (None, "env-secret"),
        ("explicit-secret", "env-secret"),
    ],
    ids=["unset", "empty", "special", "long", "env", "explicit-and-env"],
)
def test_tcp_credentials_use_private_config(
    client_process: Path,
    monkeypatch: pytest.MonkeyPatch,
    password: Optional[str],
    inherited: Optional[str],
) -> None:
    """Keep exact credentials separate from SQL and preserve their native sources."""
    if inherited is not None:
        monkeypatch.setenv("CLICKHOUSE_PASSWORD", inherited)
    port = ClickhousePort.TCP if inherited is not None else ClickhousePort.TCP_SECURE
    result = make_client(password).query(
        "SELECT 42", format_="JSON", port=port, log_query=False
    )
    assert result["query"] == "SELECT 42 FORMAT JSON"
    assert result["same_process_group"]
    assert ("--password" in result["argv"]) == (password is not None)
    expected = {}
    references = [
        (result["argv"][-1] if password is not None else None, password),
        (result["env_password"], inherited),
    ]
    for reference, value in references:
        if value is None:
            assert reference is None
        else:
            assert reference.startswith("${ch_tools_") and reference.endswith("}")
            key = reference[2:-1]
            assert key not in expected
            expected[key] = hashlib.sha256(value.encode()).hexdigest()
    assert result["hashes"] == expected
    assert os.environ.get("CLICKHOUSE_PASSWORD") == inherited
    if expected:
        assert result["secret_mode"] == 0o600
        assert result["directory_mode"] == 0o700
        assert not Path(result["config_path"]).parent.exists()
    else:
        assert result["config_path"] is None


def test_client_config_mirrors_all_entries(client_process: Path) -> None:
    """Preserve all native inputs and keep each invocation's secret fields distinct."""
    root = client_process.parent
    original = root / "clickhouse-client.xml"
    contents = "<config><password>config-secret</password><ch_tools_password>existing-field</ch_tools_password></config>"
    original.write_text(contents)
    entries = [
        "conf.d/z.xml",
        "clickhouse-client.d/a.yaml",
        "clickhouse-client.d/b.conf",
        "clickhouse-client.d/.ignored.xml",
        "clickhouse-client.d/readme.txt",
    ]
    for name in entries:
        path = root / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(
            "password: selected-secret\n" if path.suffix == ".yaml" else "<config/>"
        )
    entries.append("clickhouse-client.d/directory.xml")
    (root / entries[-1]).mkdir()
    with client_config("explicit-secret") as (args, _env, descriptors):
        config = Path(args[1])
        assert config.name == original.name
        assert config.resolve() == original
        for name in entries:
            assert (config.parent / name).is_symlink()
            assert (config.parent / name).resolve() == root / name
        key = args[-1][2:-1]
        assert key != "ch_tools_password"
        secret = config.with_suffix(".d") / f"{key}.xml"
        assert os.readlink(secret) == f"/dev/fd/{descriptors[0]}"
        with client_config("another-secret") as (other_args, _env, _fds):
            assert other_args[-1] != args[-1]
        assert original.read_text() == contents
    assert not config.parent.exists()
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(descriptors[0])


@pytest.mark.parametrize(
    "version,expected,empty_home",
    [
        ("23.3.22.3", "legacy", False),
        ("23.8.16.40", "yaml", False),
        ("26.3.12.3", "xdg", False),
        ("26.3.12.3", "xdg", True),
    ],
)
def test_client_config_search_order(
    client_process: Path,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    expected: str,
    empty_home: bool,
) -> None:
    """Match XML, YAML and XDG discovery for supported client versions."""
    root = client_process.parent
    (root / "clickhouse-client.xml").unlink()
    legacy = root / ".clickhouse-client"
    legacy.mkdir()
    (legacy / "config.xml").write_text("<config/>")
    (legacy / "config.yaml").write_text("password: unused\n")
    (root / "clickhouse-client.yaml").write_text("password: yaml\n")
    xdg = root / ".config/clickhouse"
    xdg.mkdir(parents=True)
    (xdg / "config.xml").write_text("<config/>")
    monkeypatch.setenv("TEST_CLIENT_VERSION", version)
    if empty_home:
        monkeypatch.setenv("HOME", "")
    with client_config("password") as (args, _env, _fds):
        source = Path(args[1]).resolve()
        assert source == (
            legacy / "config.xml"
            if expected == "legacy"
            else root / "clickhouse-client.yaml"
        )
    (root / "clickhouse-client.yaml").unlink()
    with client_config("password") as (args, _env, _fds):
        assert Path(args[1]).resolve() == (
            xdg / "config.xml" if expected == "xdg" else legacy / "config.xml"
        )


@pytest.mark.parametrize("password", ["\x00", "\ud800", "\n"])
def test_tcp_rejects_unsafe_config_values(client_process: Path, password: str) -> None:
    """Reject XML-invalid values and the native prompt sentinel before spawning."""
    with pytest.raises(ValueError, match="unsupported characters"):
        make_client(password).query(
            "SELECT 42", port=ClickhousePort.TCP, log_query=False
        )
    assert not client_process.exists()


def test_tcp_errors_do_not_expose_credentials(
    client_process: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep untrusted stderr and credentials out of errors and tracebacks."""
    password = "test-secret-for-errors"
    monkeypatch.setenv("TEST_CLIENT_ERROR", "1")
    with pytest.raises(RuntimeError, match="failed with exit code 42") as error:
        make_client(password).query(
            "SELECT 42", port=ClickhousePort.TCP, log_query=False
        )
    assert password not in "".join(traceback.format_exception(error.value))
    result = json.loads(client_process.read_text())
    assert not Path(result["config_path"]).parent.exists()


def test_tcp_interrupt_stops_child_before_cleanup(client_process: Path) -> None:
    """Interrupt an isolated foreground group and verify process and secret cleanup."""
    root = client_process.parent
    ready = root / "ready"
    stopped = root / "stopped"
    reaped = root / "reaped"
    os.mkfifo(ready)
    repository = Path(config_module.__file__).parents[4]
    code = f"""
import sys
import subprocess
from pathlib import Path
sys.path.insert(0, {str(repository)!r})
original_wait = subprocess.Popen.wait
marker = Path({str(reaped)!r})
def interrupt_wait(process, timeout=None):
    "Record a completed wait after one interruption during cleanup."
    if '--config-file' in process.args and timeout in (None, 5):
        if not marker.exists():
            marker.write_text('interrupted')
            raise KeyboardInterrupt
        result = original_wait(process, timeout)
        marker.write_text('reaped')
        return result
    return original_wait(process, timeout)
subprocess.Popen.wait = interrupt_wait
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient
from ch_tools.common.clickhouse.config.clickhouse import ClickhousePort
try:
    ClickhouseClient(host="localhost", password="test-password", ports={{ClickhousePort.TCP: 9000}}, timeout=5).query("SELECT 42", port=ClickhousePort.TCP, log_query=False)
except KeyboardInterrupt:
    sys.exit(130)
"""
    env = dict(
        os.environ,
        TEST_CLIENT_WAIT="1",
        TEST_CLIENT_READY=str(ready),
        TEST_CLIENT_STOPPED=str(stopped),
    )
    reader = os.open(ready, os.O_RDONLY | os.O_NONBLOCK)
    child_pid = None
    try:
        with subprocess.Popen(
            [sys.executable, "-c", code],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        ) as parent:
            try:
                assert select.select([reader], [], [], 10)[
                    0
                ], "Client did not become ready"
                assert os.read(reader, 4096) == b"ready\n"
                report = json.loads(client_process.read_text())
                child_pid = report["pid"]
                assert report["same_process_group"]
                os.killpg(parent.pid, signal.SIGINT)
                parent.communicate(timeout=10)
                assert parent.returncode == 130
                assert stopped.exists()
                assert reaped.read_text() == "reaped"
                with pytest.raises(ProcessLookupError):
                    os.kill(child_pid, 0)
                assert not Path(report["config_path"]).parent.exists()
            finally:
                if parent.poll() is None:
                    parent.kill()
                    parent.wait()
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
    finally:
        os.close(reader)
