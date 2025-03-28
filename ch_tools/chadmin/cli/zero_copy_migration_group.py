import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, NamedTuple, Optional, Set, Tuple

import yaml
from click import Context, group, option, pass_context
from cloup.constraints import AcceptAtMost, constraint

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.cli.wait_group import is_clickhouse_alive
from ch_tools.chadmin.internal.system import get_version
from ch_tools.chadmin.internal.table_replica import restore_replica
from ch_tools.chadmin.internal.utils import execute_query, remove_from_disk
from ch_tools.chadmin.internal.zookeeper import delete_zk_nodes
from ch_tools.common import logging
from ch_tools.common.cli.formatting import print_response
from ch_tools.common.clickhouse.client import OutputFormat
from ch_tools.common.clickhouse.config import get_clickhouse_config
from ch_tools.common.clickhouse.config.storage_configuration import S3DiskConfiguration
from ch_tools.common.process_pool import WorkerTask, execute_tasks_in_parallel


class TablePartition(NamedTuple):
    table: str
    partition: str


@group("zero-copy-migration", cls=Chadmin)
def zero_copy_migration_group():
    """
    Commands for manipulating data stored by ClickHouse.
    """
    pass


DEFAULT_STATUS_PATH = "/tmp/zc-migration-file.yaml"
QUERY_TIMEOUT = 7200
RETRY_COUNT = 10


class STATUS(Enum):
    INIT = 1
    DETACHED = 2
    ZK_CLEANED = 3
    RESTORED = 4


@dataclass
class TableMeta:
    db: str
    table: str
    status: STATUS
    zk_path: str


def meta_to_dict(meta):
    return {
        "db": meta.db,
        "table": meta.table,
        "status": meta.status.value,
        "zk_path": meta.zk_path,
    }


def dict_to_meta(d):
    return TableMeta(d["db"], d["table"], STATUS(d["status"]), d["zk_path"])


def update_status_file(status_file_path, tables_stats):
    with open(status_file_path, "w") as file:
        meta = [meta_to_dict(x) for x in tables_stats]
        file.write(yaml.dump(meta))


@zero_copy_migration_group.command("migrate")
@pass_context
@option(
    "--status-file-path",
    "status_file_path",
    default=DEFAULT_STATUS_PATH,
    help="Additional check: specified COLUMN name should exists in data to be removed. Example: `initial_query_start_time_microseconds.bin` for `query_log`-table.",
)
@option(
    "--not-dry-run",
    "dry_run",
    is_flag=True,
    default=True,
    help="Get merges from all hosts in the cluster.",
)
def migrate(ctx, status_file_path, dry_run):
    print(Path(status_file_path).exists())
    if not Path(status_file_path).exists():
        generate_status_file(ctx, status_file_path)

    tables_stat = load_statuses(ctx, status_file_path)
    print(tables_stat)

    detach_tables(ctx, tables_stat, status_file_path, dry_run)
    tables_stat = load_statuses(ctx, status_file_path)
    print(tables_stat)

    remove_zk_nodes(ctx, tables_stat, status_file_path, dry_run)
    tables_stat = load_statuses(ctx, status_file_path)
    print(tables_stat)
    restart_clickhouse_server(ctx, tables_stat, dry_run)

    restore_replica_step(ctx, tables_stat, status_file_path, dry_run)
    tables_stat = load_statuses(ctx, status_file_path)
    print(tables_stat)


def generate_status_file(ctx, status_file_path):
    logging.info("Generate status file")

    tables_to_migrage_query = """
        WITH obj AS
        (
            SELECT
                `table`,
                database
            FROM system.parts
            WHERE disk_name = 'object_storage'
            GROUP BY
                `database`,
                `table`
        )
        SELECT
            database,
            name
        FROM system.tables
        INNER JOIN obj USING (`table`)
        WHERE engine ILIKE '%replicated%'
    """

    resp = execute_query(
        ctx,
        tables_to_migrage_query,
        format_=OutputFormat.JSON,
    )["data"]
    print(resp)

    statuses_list = []
    for value in resp:
        database = value["database"]
        table = value["name"]
        status = STATUS.INIT
        get_zk_query = f"""
            SELECT zookeeper_path
            FROM system.replicas
            WHERE (database = '{database}') AND (`table` = '{table}')
        """
        zk_path = execute_query(ctx, get_zk_query, format_=OutputFormat.JSON)["data"][
            0
        ]["zookeeper_path"]
        statuses_list.append(TableMeta(database, table, status, zk_path))

    update_status_file(status_file_path, statuses_list)


def load_statuses(ctx, status_file_path):
    logging.info("Load status file")

    with open(status_file_path, "r") as file:
        loaded = yaml.load(file, yaml.SafeLoader)
        return [dict_to_meta(x) for x in loaded]


def execute_query_with_retry(ctx, query, dry_run=True):
    for _ in range(RETRY_COUNT):
        try:
            execute_query(
                ctx,
                query,
                timeout=QUERY_TIMEOUT,
                dry_run=dry_run,
            )
            return
        except Exception:
            pass


def detach_tables(ctx, tables_stat, status_file_path, dry_run=True):
    logging.info("Detach tables step")
    for index in range(len(tables_stat)):
        if tables_stat[index].status == STATUS.INIT:
            execute_query_with_retry(
                ctx,
                query=f"DETACH TABLE `{tables_stat[index].db}`.`{tables_stat[index].table}`",
            )
            tables_stat[index].status = STATUS.DETACHED
            update_status_file(status_file_path, tables_stat)


def remove_zk_nodes(ctx, tables_stat, status_file_path, dry_run=True):
    logging.info("Remove from zk step")
    for index in range(len(tables_stat)):
        if tables_stat[index].status == STATUS.DETACHED:
            delete_zk_nodes(ctx, [tables_stat[index].zk_path], dry_run=dry_run)
            tables_stat[index].status = STATUS.ZK_CLEANED
            update_status_file(status_file_path, tables_stat)


def restart_clickhouse_server(ctx, tables_stat, dry_run=True):
    logging.info("Restart clickhouse step")
    for table in tables_stat:
        if table.status == STATUS.ZK_CLEANED:
            restart_cmd = "service clickhouse-server restart"
            logging.info(restart_cmd)
            if dry_run:
                return

            subprocess.run(restart_cmd, shell=True)

            deadline = time.time() + QUERY_TIMEOUT

            ch_is_alive = False
            while time.time() < deadline:
                if is_clickhouse_alive():
                    ch_is_alive = True
                    break
                time.sleep(1)
            if not ch_is_alive:
                logging.error("CH is dead")
                exit(1)
            return


def restore_replica_step(ctx, tables_stat, status_file_path, dry_run=True):
    logging.info("Remove from zk step")

    for index in range(len(tables_stat)):
        if tables_stat[index].status == STATUS.ZK_CLEANED:
            restore_replica(
                ctx,
                tables_stat[index].db,
                tables_stat[index].table,
                cluster=None,
                dry_run=dry_run,
            )
            tables_stat[index].status = STATUS.RESTORED
            update_status_file(status_file_path, tables_stat)
