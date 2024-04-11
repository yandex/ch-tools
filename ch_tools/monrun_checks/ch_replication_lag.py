import click

from ch_tools.common.commands.replication_lag import estimate_replication_lag


@click.command("replication-lag")
@click.option(
    "-x",
    "--exec-critical",
    "xcrit",
    type=int,
    help="Critical threshold for one task execution.",
)
@click.option(
    "-c",
    "--critical",
    "crit",
    type=int,
    help="Critical threshold for lag with errors.",
)
@click.option("-w", "--warning", "warn", type=int, help="Warning threshold.")
@click.option(
    "-M",
    "--merges-critical",
    "mcrit",
    type=click.FloatRange(0.0, 100.0),
    help="Critical threshold in percent of max_replicated_merges_in_queue.",
)
@click.option(
    "-m",
    "--merges-warning",
    "mwarn",
    type=click.FloatRange(0.0, 100.0),
    help="Warning threshold in percent of max_replicated_merges_in_queue.",
)
@click.option(
    "-v",
    "--verbose",
    "verbose",
    type=int,
    count=True,
    default=0,
    help="Show details about lag.",
)
@click.pass_context
def replication_lag_command(ctx, xcrit, crit, warn, mwarn, mcrit, verbose):
    return estimate_replication_lag(ctx, xcrit, crit, warn, mwarn, mcrit, verbose)
