import os
import re
import socket
import ssl
import time
from typing import Any, Dict, List, Optional, Tuple

from click import Context, command, option, pass_context
from kazoo.client import KazooClient
from kazoo.security import ACL, make_digest_acl

from ch_tools.common.clickhouse.config import ClickhouseKeeperConfig
from ch_tools.common.result import CRIT, OK, WARNING, Result
from ch_tools.common.tls import check_cert_on_ports

ZOOKEEPER_CFG_FILE = "/etc/zookeeper/conf/zoo.cfg"
DEFAULT_ZOOKEEPER_DATA_DIR = "/var/lib/zookeeper"
DEFAULT_ZOOKEEPER_DATA_LOG_DIR = "/var/log/zookeeper"
KEEPER_DEFAULT_PATH = "/var/lib/clickhouse-keeper/snapshots"
CH_DBMS_DEFAULT_PATH = "/var/lib/clickhouse/snapshots"

context = ssl.create_default_context()


@command("alive")
@pass_context
def alive_command(ctx: Context) -> Result:
    """Check (Zoo)Keeper service is alive"""
    try:
        keeper_port, use_ssl = get_keeper_port_pair()
        username = ctx.obj["config"]["zookeeper"]["username"]
        password = ctx.obj["config"]["zookeeper"]["password"]
        args: Dict[str, Any] = {
            "hosts": f"127.0.0.1:{keeper_port}",
            "connection_retry": ctx.obj.get("retries"),
            "command_retry": ctx.obj.get("retries"),
            "timeout": ctx.obj.get("timeout"),
            "use_ssl": use_ssl,
            "verify_certs": not ctx.obj.get("no_verify_ssl_certs"),
        }
        if username is not None and password is not None:
            auth_data = [
                (
                    "digest",
                    f"{username}:{password}",
                )
            ]
            acls: List[ACL] = [make_digest_acl(username, password, all=True)]
            args["auth_data"] = auth_data
            args["default_acl"] = acls

        client = KazooClient(**args)
        client.start()
        client.get("/")
        client.create(path=f"/{socket.getfqdn()}_alive", ephemeral=True)
        client.stop()
        client.close()
    except Exception as e:
        return Result(CRIT, repr(e))

    return Result(OK)


@command("avg_latency")
@pass_context
def avg_latency_command(ctx: Context) -> Result:
    """Check average (Zoo)Keeper latency"""
    return Result(OK, keeper_mntr(ctx)["zk_avg_latency"])


@command("min_latency")
@pass_context
def min_latency_command(ctx: Context) -> Result:
    """Check minimum (Zoo)Keeper latency"""
    return Result(OK, keeper_mntr(ctx)["zk_min_latency"])


@command("max_latency")
@pass_context
def max_latency_command(ctx: Context) -> Result:
    """Check maximum (Zoo)Keeper latency"""
    return Result(OK, keeper_mntr(ctx)["zk_max_latency"])


@command("queue")
@pass_context
def queue_command(ctx: Context) -> Result:
    """Check number of queued requests on (Zoo)Keeper server"""
    return Result(OK, keeper_mntr(ctx)["zk_outstanding_requests"])


@command("descriptors")
@pass_context
def descriptors_command(ctx: Context) -> Result:
    """Check number of open file descriptors on (Zoo)Keeper server"""
    return Result(OK, keeper_mntr(ctx)["zk_open_file_descriptor_count"])


@command("version")
@pass_context
def get_version_command(ctx: Context) -> Result:
    """Check (Zoo)Keeper version"""
    return Result(OK, keeper_mntr(ctx)["zk_version"])


@command("snapshot")
def check_snapshots() -> Result:
    """Check (Zoo)Keeper snapshots"""
    latest = "No (zoo)keeper snapshots done yet"
    if os.path.exists(ZOOKEEPER_CFG_FILE):
        files = get_snapshot_files(
            read_zookeeper_config().get("dataDir", DEFAULT_ZOOKEEPER_DATA_DIR)
        )
    else:
        files = get_keeper_snapshot_files()
    if len(files) > 0:
        latest = sorted(files, key=os.path.getctime, reverse=True)[0]
    return Result(OK, latest)


@command("last_null_pointer")
def check_last_null_pointer_exc() -> Result:
    """
    Get moment from Zookeeper logs then NullPointerException appeared during last 24 hours
    """
    if not os.path.exists(ZOOKEEPER_CFG_FILE):
        return Result(OK)

    files = get_zookeeper_log_files_for_last_day()
    if len(files) == 0:
        return Result(OK)
    prev_line = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(os.path.getctime(files[0]))
    )
    latest = None
    for file in files:
        with open(file, encoding="utf-8") as f:
            for line in f:
                if "java.lang.NullPointerException" in line:
                    latest = prev_line.split("[")[0].strip()
                prev_line = line
    if latest:
        return Result(WARNING, latest)
    return Result(OK)


@command("tls")
@option("-c", "--critical", "crit", type=int, help="Critical threshold.")
@option("-w", "--warning", "warn", type=int, help="Warning threshold.")
@option(
    "-p",
    "--ports",
    "ports",
    type=str,
    default=None,
    help="Comma separated list of ports. By default read from ClickHouse config",
)
@option("--chain", "chain", is_flag=True, help="Verify certificate chain.")
def tls_command(
    crit: int,
    warn: int,
    ports: Optional[str],
    chain: bool,
) -> Result:
    """
    Check TLS certificate for expiration and that actual cert from fs used.
    """
    # pylint: disable=too-many-return-statements

    if ports:
        port_list = ports.split(",")
    else:
        port, is_secure = get_keeper_port_pair()
        if not is_secure:
            return Result(OK, "Keeper doesn't have security port.")
        port_list = [port]  # type: ignore

    path = get_keeper_cert_path()
    if not path:
        return Result(WARNING, "Keeper has security port without configured TLS cert.")

    return check_cert_on_ports(port_list, crit, warn, chain, path)


def get_zookeeper_log_files_for_last_day() -> List[str]:
    """Collect Zookeeper logs for last 24 hours"""
    current_timestamp = time.time()
    logs_path = read_zookeeper_config().get(
        "dataLogDir", DEFAULT_ZOOKEEPER_DATA_LOG_DIR
    )
    log_files = list(
        filter(
            lambda file: (current_timestamp - os.path.getmtime(file)) < 60 * 60 * 24,
            [
                os.path.join(root, name)
                for root, _, files in os.walk(logs_path)
                for name in files
                if name.endswith(".log")
            ],
        )
    )
    return sorted(log_files, key=os.path.getctime)


def get_keeper_snapshot_files() -> List[str]:
    """Perform look over all possible folders for snapshots files and grab it"""
    files: List[str] = []
    ch_config = ClickhouseKeeperConfig.load()
    dirs = [KEEPER_DEFAULT_PATH, CH_DBMS_DEFAULT_PATH, ch_config.snapshots_dir]
    if ch_config.storage_dir:
        dirs.append(os.path.join(ch_config.storage_dir, "snapshots"))
    for snapshot_path in set(dirs):
        if snapshot_path and os.path.exists(snapshot_path):
            files.extend(get_snapshot_files(snapshot_path))
    return files


def get_snapshot_files(snapshots_dir: str) -> List[str]:
    """Select snapshot files in given directory"""
    return [
        os.path.join(root, name)
        for root, _, files in os.walk(snapshots_dir)
        for name in files
        if name.startswith("snapshot")
    ]


def read_zookeeper_config() -> Dict[str, str]:
    """
    Read Zookeeper configuration file and return content as dict
    """
    config: Dict[str, str] = {}
    if not os.path.exists(ZOOKEEPER_CFG_FILE):
        return config
    with open(ZOOKEEPER_CFG_FILE, encoding="utf-8") as f:
        for line in f:
            conf_line = line.split("=")
            if len(conf_line) > 1:
                config[conf_line[0].strip()] = conf_line[1].strip()
    return config


def get_keeper_port_pair() -> Tuple[int, bool]:
    """
    :returns tuple (port for (Zoo)Keeper, port is secure).
      If no config was found, default (insecure) port 2181 is returned.
    """
    try:
        return ClickhouseKeeperConfig.load().port_pair
    except FileNotFoundError:
        return 2181, False


def get_keeper_cert_path() -> Optional[str]:
    """
    :returns path to Keeper TLS cert if exists.
    """
    try:
        return ClickhouseKeeperConfig.load().tls_cert_path
    except FileNotFoundError:
        return None


def keeper_command(cmd: str, timeout: int, verify_ssl_certs: bool) -> str:
    """
    Execute (Zoo)Keeper 4-letter command.
    """
    port, is_secure = get_keeper_port_pair()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)

        if is_secure:
            if not verify_ssl_certs:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            with context.wrap_socket(sock, server_hostname=socket.getfqdn()) as ssock:
                ssock.connect(("127.0.0.1", port))
                ssock.sendall(cmd.encode())
                return ssock.makefile().read(-1)
        else:
            sock.connect(("127.0.0.1", port))
            sock.sendall(cmd.encode())
            return sock.makefile().read(-1)


def keeper_mntr(ctx: Context) -> Dict[str, str]:
    """
    Execute (Zoo)Keeper mntr command and parse its output.
    """
    result: Dict[str, str] = {}
    attempt = 0
    while True:
        try:
            response = keeper_command(
                "mntr",
                ctx.obj.get("timeout", 3),
                not ctx.obj.get("no_verify_ssl_certs"),
            )
            for line in response.split("\n"):
                key_value = re.split("\\s+", line, 1)
                if len(key_value) == 2:
                    result[key_value[0]] = key_value[1]

            if len(result) <= 1:
                raise RuntimeError(f"Too short response: {response.strip()}")

            break
        except Exception as e:
            if attempt >= ctx.obj.get("retries", 3):
                raise e
            attempt += 1
            time.sleep(0.5)
    return result
