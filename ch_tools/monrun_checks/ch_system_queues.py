import click
import yaml

from ch_tools.common.result import Result
from ch_tools.monrun_checks.utils import execute_query


@click.command("system-queues")
@click.option(
    "-c", "--critical", "crit", type=int, default=20, help="Critical threshold."
)
@click.option(
    "-w", "--warning", "warn", type=int, default=10, help="Warning threshold."
)
@click.option(
    "-f", "--config_file", "conf", help="Config file with thresholds per each table."
)
@click.pass_context
def system_queues_command(ctx, crit, warn, conf):
    """
    Check system queues.
    """
    if conf is not None:
        config = get_config(conf)
    else:
        config = {"triggers": {"default": {"crit": crit, "warn": warn}}}

    metrics = get_metrics(ctx)
    return check_metrics(metrics, config)


def get_config(conf):
    """
    Return config.
    """
    with open(conf, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_metrics(ctx):
    """
    Select and return metrics form system.replicas.
    """
    query = (
        "SELECT database, table, future_parts, parts_to_check, queue_size,"
        " inserts_in_queue, merges_in_queue FROM system.replicas"
    )
    return execute_query(ctx, query=query, compact=False)


def check_metrics(metrics, config):
    """
    Check that metrics are within acceptable levels.
    """
    thresholds_conf = config["triggers"]
    default_thresholds = thresholds_conf["default"]
    status_map = {"crit": 2, "warn": 1}

    status = 0
    message = "OK"
    triggers = []

    for row in metrics:
        db_table = "{}.{}".format(row["database"], row["table"])
        table_thresholds = thresholds_conf.get(db_table, {})

        for key, value in row.items():
            if key not in default_thresholds:
                continue

            thresholds = table_thresholds.get(key, default_thresholds[key])

            report = ""
            for prior in "crit", "warn":
                threshold = thresholds[prior]
                if value > threshold:
                    table_status = status_map[prior]
                    report += "{}: {} {} > {} ({});".format(
                        db_table, key, value, threshold, prior
                    )
                    triggers.append((table_status, report))
                    break

    if triggers:
        triggers.sort(reverse=True, key=lambda x: x[0])
        status = triggers[0][0]
        message = " ".join(x[1] for x in triggers)

    return Result(code=status, message=message)
