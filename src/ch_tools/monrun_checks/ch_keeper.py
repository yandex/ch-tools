import cloup
from kazoo.client import KazooClient, KazooException
from kazoo.handlers.threading import KazooTimeoutError

from ch_tools.common.clickhouse.config import ClickhouseKeeperConfig
from ch_tools.common.result import Result


@cloup.command("keeper")
@cloup.option(
    "-r",
    "--retries",
    "retries",
    type=int,
    default=3,
    help="Connection retries",
)
@cloup.option(
    "-t",
    "--timeout",
    "timeout",
    type=int,
    default=10,
    help="Connection timeout (s)",
)
@cloup.option(
    "-n",
    "--no-verify-ssl-certs",
    "no_verify_ssl_certs",
    is_flag=True,
    default=False,
    help="Allow unverified SSL certificates, e.g. self-signed ones",
)
def keeper_command(retries: int, timeout: int, no_verify_ssl_certs: bool) -> Result:
    """
    Check ClickHouse Keeper is alive.
    """
    keeper_port, use_ssl = ClickhouseKeeperConfig.load().port_pair
    if not keeper_port:
        return Result(0, "Disabled")

    client = KazooClient(
        f"127.0.0.1:{keeper_port}",
        connection_retry=retries,
        command_retry=retries,
        timeout=timeout,
        use_ssl=use_ssl,
        verify_certs=not no_verify_ssl_certs,
    )
    try:
        client.start()
        client.get("/")
        client.stop()
    except (KazooException, KazooTimeoutError) as e:
        return Result(2, repr(e))

    return Result(0, "OK")
