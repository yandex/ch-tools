from datetime import datetime

from click import Context

from ch_tools.chadmin.internal.diagnostics import formatter, query
from ch_tools.common.cli.formatting import format_duration
from ch_tools.common.cli.progress_bar import progress
from ch_tools.common.clickhouse.client import OutputFormat
from ch_tools.common.clickhouse.config import (
    ClickhouseConfig,
    ClickhouseKeeperConfig,
    ClickhouseUsersConfig,
)
from ch_tools.common.dbaas import DbaasConfig
from ch_tools.common.utils import version_ge

from ..utils import clickhouse_client
from .data import DiagnosticsData, add_command, add_query, execute_query


def diagnose(ctx: Context, output_format: str, normalize_queries: bool) -> None:
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    client = clickhouse_client(ctx)
    dbaas_config = DbaasConfig.load()
    ch_config = ClickhouseConfig.load()
    keeper_config = ClickhouseKeeperConfig.load()
    ch_users_config = ClickhouseUsersConfig.load()
    version = client.get_clickhouse_version()
    system_tables = [
        row[0]
        for row in execute_query(
            client, query.SELECT_SYSTEM_TABLES, format_=OutputFormat.JSONCompact
        )["data"]
    ]

    diagnostics = DiagnosticsData(
        host=dbaas_config.fqdn, normalize_queries=normalize_queries
    )

    tasks = [
        diagnostics.add_string("Cluster ID", dbaas_config.cluster_id),
        diagnostics.add_string("Cluster name", dbaas_config.cluster_name),
        diagnostics.add_string("Version", version),
        diagnostics.add_string("Host", dbaas_config.fqdn),
        diagnostics.add_string("Replicas", ",".join(dbaas_config.replicas)),
        diagnostics.add_string("Shard count", dbaas_config.shard_count),
        diagnostics.add_string("Host count", dbaas_config.host_count),
        diagnostics.add_string("Virtualization", dbaas_config.vtype),
        diagnostics.add_string(
            "Resource preset", formatter.format_resource_preset(dbaas_config)
        ),
        diagnostics.add_string(
            "Storage", formatter.format_storage(dbaas_config, ch_config)
        ),
        diagnostics.add_string("Timestamp", timestamp),
        diagnostics.add_string("Uptime", format_duration(client.get_uptime())),
        diagnostics.add_xml_document("ClickHouse configuration", ch_config.dump_xml()),
    ]

    if keeper_config.separated:
        tasks.append(
            diagnostics.add_xml_document(
                "ClickHouse Keeper configuration", keeper_config.dump_xml()
            )
        )

    tasks.extend(
        [
            diagnostics.add_xml_document(
                "ClickHouse users configuration", ch_users_config.dump_xml()
            ),
            add_query(
                diagnostics,
                "Access configuration",
                client=client,
                query=query.SELECT_ACCESS,
                format_=OutputFormat.TabSeparatedRaw,
            ),
            add_query(
                diagnostics,
                "Quotas",
                client=client,
                query=query.SELECT_QUOTA_USAGE,
                format_=OutputFormat.Vertical,
            ),
            add_query(
                diagnostics,
                "Database engines",
                client=client,
                query=query.SELECT_DATABASE_ENGINES,
                format_=OutputFormat.PrettyCompactNoEscapes,
                section="Schema",
            ),
            add_query(
                diagnostics,
                "Databases (top 10 by size)",
                client=client,
                query=query.SELECT_DATABASES,
                format_=OutputFormat.PrettyCompactNoEscapes,
                section="Schema",
            ),
            add_query(
                diagnostics,
                "Table engines",
                client=client,
                query=query.SELECT_TABLE_ENGINES,
                format_=OutputFormat.PrettyCompactNoEscapes,
                section="Schema",
            ),
            add_query(
                diagnostics,
                "Dictionaries",
                client=client,
                query=query.SELECT_DICTIONARIES,
                format_=OutputFormat.PrettyCompactNoEscapes,
                section="Schema",
            ),
            add_query(
                diagnostics,
                "Replicated tables (top 10 by absolute delay)",
                client=client,
                query=query.SELECT_REPLICAS,
                format_=OutputFormat.PrettyCompactNoEscapes,
                section="Replication",
            ),
            add_query(
                diagnostics,
                "Replication queue (top 20 oldest tasks)",
                client=client,
                query=query.SELECT_REPLICATION_QUEUE,
                format_=OutputFormat.Vertical,
                section="Replication",
            ),
        ]
    )

    if version_ge(version, "21.3"):
        tasks.append(
            add_query(
                diagnostics,
                "Replicated fetches",
                client=client,
                query=query.SELECT_REPLICATED_FETCHES,
                format_=OutputFormat.Vertical,
                section="Replication",
            ),
        )

    tasks.extend(
        [
            add_query(
                diagnostics,
                "Top 10 tables by max parts per partition",
                client=client,
                query=query.SELECT_PARTS_PER_TABLE,
                format_=OutputFormat.PrettyCompactNoEscapes,
            ),
            add_query(
                diagnostics,
                "Merges in progress",
                client=client,
                query=query.SELECT_MERGES,
                format_=OutputFormat.Vertical,
            ),
            add_query(
                diagnostics,
                "Mutations in progress",
                client=client,
                query=query.SELECT_MUTATIONS,
                format_=OutputFormat.Vertical,
            ),
            add_query(
                diagnostics,
                "Recent data parts (modification time within last 3 minutes)",
                client=client,
                query=query.SELECT_RECENT_DATA_PARTS,
                format_=OutputFormat.Vertical,
            ),
            add_query(
                diagnostics,
                "system.detached_parts",
                client=client,
                query=query.SELECT_DETACHED_DATA_PARTS,
                format_=OutputFormat.PrettyCompactNoEscapes,
                section="Detached data",
            ),
            add_command(
                diagnostics,
                "Disk space usage",
                # language=sh
                command="du -sh -L -c /var/lib/clickhouse/data/*/*/detached/* | sort -rsh",
                section="Detached data",
            ),
            add_query(
                diagnostics,
                "Queries in progress (process list)",
                client=client,
                query=query.SELECT_PROCESSES,
                format_=OutputFormat.Vertical,
                section="Queries",
            ),
        ]
    )

    if "query_log" in system_tables:
        tasks.extend(
            [
                add_query(
                    diagnostics,
                    "Top 10 queries by duration",
                    client=client,
                    query=query.SELECT_TOP_QUERIES_BY_DURATION,
                    format_=OutputFormat.Vertical,
                    section="Queries",
                ),
                add_query(
                    diagnostics,
                    "Top 10 queries by memory usage",
                    client=client,
                    query=query.SELECT_TOP_QUERIES_BY_MEMORY_USAGE,
                    format_=OutputFormat.Vertical,
                    section="Queries",
                ),
                add_query(
                    diagnostics,
                    "Last 10 failed queries",
                    client=client,
                    query=query.SELECT_FAILED_QUERIES,
                    format_=OutputFormat.Vertical,
                    section="Queries",
                ),
            ]
        )

    tasks.append(
        add_query(
            diagnostics,
            "Stack traces",
            client=client,
            query=query.SELECT_STACK_TRACES,
            format_=OutputFormat.Vertical,
        ),
    )

    if "crash_log" in system_tables:
        tasks.append(
            add_query(
                diagnostics,
                "Crash log",
                client=client,
                query=query.SELECT_CRASH_LOG,
                format_=OutputFormat.Vertical,
            ),
        )

    tasks.append(
        add_command(
            diagnostics,
            "lsof",
            # language=sh
            "lsof -p $(pidof clickhouse-server)",
        )
    )

    for task in progress(tasks, description="Performing diagnostics"):
        task()

    diagnostics.dump(output_format)
