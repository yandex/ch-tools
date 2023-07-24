import os
import re
import socket
import ssl
import time
from typing import Dict

from click import command, pass_context
from kazoo.client import KazooClient

from ch_tools.common.clickhouse.config import ClickhouseKeeperConfig
from ch_tools.common.result import Result

ZOOKEEPER_CFG_FILE = "/etc/zookeeper/conf/zoo.cfg"
DEFAULT_ZOOKEEPER_DATA_DIR = "/var/lib/zookeeper"
DEFAULT_ZOOKEEPER_DATA_LOG_DIR = "/var/log/zookeeper"
KEEPER_DEFAULT_PATH = "/var/lib/clickhouse-keeper/snapshots"
CH_DBMS_DEFAULT_PATH = "/var/lib/clickhouse/snapshots"

context = ssl.create_default_context()


@command("alive")
@pass_context
def alive_command(ctx):
    """Check (Zoo)Keeper service is alive"""
    try:
        keeper_port, use_ssl = get_keeper_port_pair()
        client = KazooClient(
            f"127.0.0.1:{keeper_port}",
            connection_retry=ctx.obj.get("retries"),
            command_retry=ctx.obj.get("retries"),
            timeout=ctx.obj.get("timeout"),
            use_ssl=use_ssl,
            verify_certs=not ctx.obj.get("no_verify_ssl_certs"),
        )
        client.start()
        client.get("/")
        client.create(path="/{0}_alive".format(socket.getfqdn()), ephemeral=True)
        client.stop()
        client.close()
    except Exception as e:
        return Result(2, repr(e))

    return Result(0, "OK")


@command("avg_latency")
@pass_context
def avg_latency_command(ctx):
    """Check average (Zoo)Keeper latency"""
    return Result(0, keeper_mntr(ctx)["zk_avg_latency"])


@command("min_latency")
@pass_context
def min_latency_command(ctx):
    """Check minimum (Zoo)Keeper latency"""
    return Result(0, keeper_mntr(ctx)["zk_min_latency"])


@command("max_latency")
@pass_context
def max_latency_command(ctx):
    """Check maximum (Zoo)Keeper latency"""
    return Result(0, keeper_mntr(ctx)["zk_max_latency"])


@command("queue")
@pass_context
def queue_command(ctx):
    """Check number of queued requests on (Zoo)Keeper server"""
    return Result(0, keeper_mntr(ctx)["zk_outstanding_requests"])


@command("descriptors")
@pass_context
def descriptors_command(ctx):
    """Check number of open file descriptors on (Zoo)Keeper server"""
    return Result(0, keeper_mntr(ctx)["zk_open_file_descriptor_count"])


@command("version")
@pass_context
def get_version_command(ctx):
    """Check (Zoo)Keeper version"""
    return Result(0, keeper_mntr(ctx)["zk_version"])


@command("snapshot")
def check_snapshots():
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
    return Result(0, latest)


@command("last_null_pointer")
def check_last_null_pointer_exc():
    """
    Get moment from Zookeeper logs then NullPointerException appeared during last 24 hours
    """
    if not os.path.exists(ZOOKEEPER_CFG_FILE):
        return Result(0, "OK")

    files = get_zookeeper_log_files_for_last_day()
    if len(files) == 0:
        return Result(0, "OK")
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
        return Result(1, latest)
    return Result(0, "OK")


def get_zookeeper_log_files_for_last_day():
    """Collect Zookeeper logs for last 24 hours"""
    current_timestamp = time.time()
    logs_path = read_zookeeper_config().get(
        "dataLogDir", DEFAULT_ZOOKEEPER_DATA_LOG_DIR
    )
    log_files = filter(
        lambda file: (current_timestamp - os.path.getmtime(file)) < 60 * 60 * 24,
        [
            os.path.join(root, name)
            for root, _, files in os.walk(logs_path)
            for name in files
            if name.endswith(".log")
        ],
    )
    return sorted(log_files, key=os.path.getctime)


def get_keeper_snapshot_files():
    """Perform look over all possible folders for snapshots files and grab it"""
    files = []
    ch_config = ClickhouseKeeperConfig.load()
    dirs = [KEEPER_DEFAULT_PATH, CH_DBMS_DEFAULT_PATH, ch_config.snapshots_dir]
    if ch_config.storage_dir:
        dirs.append(os.path.join(ch_config.storage_dir, "snapshots"))
    for snapshot_path in set(dirs):
        if snapshot_path and os.path.exists(snapshot_path):
            files.extend(get_snapshot_files(snapshot_path))
    return files


def get_snapshot_files(snapshots_dir):
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


def get_keeper_port_pair():
    """
    :returns tuple (port for (Zoo)Keeper, port is secure).
      If no config was found, default (insecure) port 2181 is returned.
    """
    try:
        return ClickhouseKeeperConfig.load().port_pair
    except FileNotFoundError:
        return 2181, False


def keeper_command(cmd, timeout, verify_ssl_certs):
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
            with context.wrap_socket(sock, server_hostname="localhost") as ssock:
                ssock.connect(("localhost", port))
                ssock.sendall(cmd.encode())
                return ssock.makefile().read(-1)
        else:
            sock.connect(("localhost", port))
            sock.sendall(cmd.encode())
            return sock.makefile().read(-1)


def keeper_mntr(ctx):
    """
    Execute (Zoo)Keeper mntr command and parse its output.
    """
    result = {}
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
