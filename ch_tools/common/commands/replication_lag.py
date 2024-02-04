from typing import Any, Dict

from tabulate import tabulate

from ch_tools.common.result import Result
from ch_tools.monrun_checks.clickhouse_client import ClickhouseClient

XCRIT = 3600
CRIT = 600
WARN = 300
MWARN = 50.0
MCRIT = 90.0
VERBOSE = 0


def estimate_replication_lag(
    ctx, xcrit=XCRIT, crit=CRIT, warn=WARN, mwarn=MWARN, mcrit=MCRIT, verbose=VERBOSE
):
    """
    Check for replication lag across replicas.
    Should be: lag >= lag_with_errors, lag >= max_execution
    """
    # pylint: disable=too-many-branches,too-many-locals
    ch_client = ClickhouseClient(ctx)
    lag, lag_with_errors, max_execution, max_merges, chart = get_replication_lag(
        ch_client
    )

    msg_verbose = ""
    msg_verbose_2 = "\n\n"

    if verbose >= 1:
        verbtab = []

        headers = [
            "Table",
            "Lag [s]",
            "Tasks",
            "Max task execution [s]",
            "Non-retrayable errors",
            "Has user fault errors",
            "Merges with 1000+ tries",
        ]
        for key, item in chart.items():
            if item.get("multi_replicas", False):
                tabletab = [
                    key,
                    item.get("delay", 0),
                    item.get("tasks", 0),
                    item.get("max_execution", 0),
                    item.get("errors", 0),
                    item.get("user_fault", False),
                    item.get("retried_merges", 0),
                ]
                verbtab.append(tabletab)
                if verbose >= 2:
                    exceptions_retrayable = ""
                    exceptions_non_retrayable = ""
                    exceptions_ignored = ""
                    for exception in item.get("exceptions", []):
                        if exception:
                            if is_userfault_exception(exception):
                                exceptions_ignored += "\t" + exception[5:] + "\n"
                            elif exception.startswith("<pr> "):
                                exceptions_retrayable += "\t" + exception[5:] + "\n"
                            else:
                                exceptions_non_retrayable += "\t" + exception[5:] + "\n"
                    max_execution_part = (
                        item.get("max_execution_part", "")
                        if item.get("max_execution", 0)
                        else 0
                    )
                    if (
                        exceptions_retrayable
                        or exceptions_non_retrayable
                        or exceptions_ignored
                        or max_execution_part
                    ):
                        msg_verbose_2 = msg_verbose_2 + key + ":\n"
                    if exceptions_non_retrayable:
                        msg_verbose_2 = (
                            msg_verbose_2
                            + "  Non-retrayable errors:\n"
                            + exceptions_non_retrayable
                        )
                    if exceptions_retrayable:
                        msg_verbose_2 = (
                            msg_verbose_2
                            + "  Retrayable errors:\n"
                            + exceptions_retrayable
                        )
                    if exceptions_ignored:
                        msg_verbose_2 = (
                            msg_verbose_2
                            + "  User fault errors:\n"
                            + exceptions_ignored
                        )
                    if max_execution_part:
                        msg_verbose_2 = (
                            msg_verbose_2
                            + "  Result part of task with max execution time: "
                            + max_execution_part
                            + "\n"
                        )
        msg_verbose = tabulate(verbtab, headers=headers)
        if verbose >= 2:
            msg_verbose = msg_verbose + msg_verbose_2

    max_merges_warn_threshold = 1
    max_merges_crit_threshold = 1
    if max_merges > 0:
        max_replicated_merges_in_queue = get_max_replicated_merges_in_queue(ch_client)
        max_merges_warn_threshold = int(max_replicated_merges_in_queue * mwarn / 100.0)
        max_merges_crit_threshold = int(max_replicated_merges_in_queue * mcrit / 100.0)

    if lag < warn and max_merges < max_merges_warn_threshold:
        return Result(code=0, message="OK", verbose=msg_verbose)

    msg = "Max {0} seconds, with errors {1} seconds, max task execution {2} seconds, max merges in queue {3}".format(
        lag, lag_with_errors, max_execution, max_merges
    )

    if (
        lag_with_errors < crit
        and max_execution < xcrit
        and max_merges < max_merges_crit_threshold
    ):
        return Result(code=1, message=msg, verbose=msg_verbose)

    return Result(code=2, message=msg, verbose=msg_verbose)


def get_replication_lag(ch_client):
    """
    Get max absolute_delay from system.replicas.
    """

    tables = get_tables_with_replication_delay(ch_client)
    chart: Dict[str, Dict[str, Any]] = {}
    for t in tables:
        key = "{database}.{table}".format(database=t["database"], table=t["table"])
        chart[key] = {}
        chart[key]["delay"] = int(t["absolute_delay"])
    tables = filter_out_single_replica_tables(ch_client, tables)
    for t in tables:
        key = "{database}.{table}".format(database=t["database"], table=t["table"])
        chart[key]["multi_replicas"] = True
    tables = count_errors(ch_client, tables, -1)

    max_merges = 0
    for t in tables:
        key = "{database}.{table}".format(database=t["database"], table=t["table"])
        chart[key]["tasks"] = int(t["tasks"])
        chart[key]["errors"] = int(t["errors"])
        chart[key]["max_execution"] = int(t["max_execution"])
        chart[key]["max_execution_part"] = t["max_execution_part"]
        chart[key]["exceptions"] = t["exceptions"]
        chart[key]["retried_merges"] = int(t["retried_merges"])
        max_merges = max(int(t["retried_merges"]), max_merges)
        for exception in t["exceptions"]:
            if is_userfault_exception(exception):
                chart[key]["userfault"] = True
                break

    lag = 0
    lag_with_errors = 0
    max_execution = 0
    for key, item in chart.items():
        if item.get("multi_replicas", False):
            delay = item.get("delay", 0)
            if delay > lag:
                lag = delay
            if (
                delay > lag_with_errors
                and item.get("errors", 0) > 0
                and not item.get("userfault", False)
            ):
                lag_with_errors = delay
            execution = item.get("max_execution", 0)
            if execution > max_execution:
                max_execution = execution

    return lag, lag_with_errors, max_execution, max_merges, chart


def get_tables_with_replication_delay(ch_client):
    """
    Get tables with absolute_delay > 0.
    """
    query = "SELECT database, table, zookeeper_path, absolute_delay FROM system.replicas WHERE absolute_delay > 0"
    return ch_client.execute(query=query, compact=False)


def filter_out_single_replica_tables(ch_client, tables):
    if not tables:
        return tables

    query = """
        SELECT
            database,
            table,
            zookeeper_path
        FROM system.replicas
        WHERE (database, table) IN ({tables})
        AND total_replicas > 1
        """.format(
        tables=",".join(
            "('{0}', '{1}')".format(t["database"], t["table"]) for t in tables
        )
    )
    return ch_client.execute(query=query, compact=False)


def count_errors(ch_client, tables, exceptions_limit):
    """
    Add count of replication errors.
    """
    if not tables:
        return tables

    limit = "" if exceptions_limit < 0 else "({})".format(exceptions_limit)

    query = """
        SELECT
            database,
            table,
            count() as tasks,
            countIf(last_exception != '' AND postpone_reason = '') as errors,
            max(IF(is_currently_executing, dateDiff('second', last_attempt_time, now()), 0)) as max_execution,
            groupUniqArray{limit}(IF(last_exception != '', concat(IF(postpone_reason = '', '     ', '<pr> '), last_exception), '')) as exceptions,
            argMax(new_part_name, IF(is_currently_executing, dateDiff('second', last_attempt_time, now()), 0)) as max_execution_part,
            countIf(type = 'MERGE_PARTS' and num_tries >= 1000) as retried_merges
        FROM system.replication_queue
        WHERE (database, table) IN ({tables})
        GROUP BY database,table
        """.format(
        tables=",".join(
            "('{0}', '{1}')".format(t["database"], t["table"]) for t in tables
        ),
        limit=limit,
    )
    return ch_client.execute(query=query, compact=False)


def is_userfault_exception(exception):
    """
    Check if exception was caused by user.
    Current list:
      * DB::Exception: Cannot reserve 1.00 MiB, not enough space
      * DB::Exception: Incorrect data: Sign = -127 (must be 1 or -1)
    """

    if "DB::Exception: Cannot reserve" in exception and "not enough space" in exception:
        return True
    if (
        "DB::Exception: Incorrect data: Sign" in exception
        and "(must be 1 or -1)" in exception
    ):
        return True

    return False


def get_max_replicated_merges_in_queue(ch_client):
    """
    Get max_replicated_merges_in_queue value
    """
    query = """
        SELECT value FROM system.merge_tree_settings WHERE name='max_replicated_merges_in_queue'
    """
    res = ch_client.execute(query=query, compact=True)
    if not res:
        return (
            16  # 16 is default value for 'max_replicated_merges_in_queue' in ClickHouse
        )
    return int(res[0][0])
