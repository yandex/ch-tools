from click import argument, group, option, pass_context

from ch_tools.chadmin.cli.chadmin_group import Chadmin
from ch_tools.chadmin.internal.utils import execute_query
from ch_tools.common import logging
from ch_tools.common.clickhouse.config import get_cluster_name


@group("mutation", cls=Chadmin)
def mutation_group():
    """
    Commands to manage mutations.
    """
    pass


@mutation_group.command("get")
@argument("mutation", required=False)
@option("--last", is_flag=True)
@pass_context
def get_mutation(ctx, mutation, last):
    """Get mutation."""
    if bool(mutation) == bool(last):
        ctx.fail("Mutation must be specified.")

    query = """
        SELECT *
        FROM system.mutations
        {% if mutation %}
        WHERE mutation_id = '{{ mutation }}'
        {% else %}
        ORDER BY mutation_id DESC
        LIMIT 1
        {% endif %}
        """
    logging.info(execute_query(ctx, query, mutation=mutation, format_="Vertical"))


@mutation_group.command("list")
@option(
    "--completed/--incompleted",
    "is_done",
    default=None,
    help="Output only completed / incompleted mutations.",
)
@option(
    "--command",
    "command_pattern",
    help="Filter mutations to output by command pattern.",
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Get mutations from all hosts in the cluster.",
)
@option(
    "-l", "--limit", type=int, help="Limit the max number of objects in the output."
)
@pass_context
def list_mutations(ctx, is_done, command_pattern, on_cluster, limit):
    """List mutations."""
    cluster = get_cluster_name(ctx) if on_cluster else None
    query = """
        SELECT
        {% if cluster %}
            hostName() "host",
        {% endif %}
            database,
            table,
            mutation_id,
            command,
            create_time,
            parts_to_do,
            is_done,
            latest_failed_part,
            latest_fail_time,
            latest_fail_reason
        {% if cluster %}
        FROM clusterAllReplicas({{ cluster }}, system.mutations)
        {% else %}
        FROM system.mutations
        {% endif %}
        WHERE 1
        {% if is_done is true %}
          AND is_done
        {% elif is_done is false %}
          AND NOT is_done
        {% endif %}
        {% if command_pattern %}
          AND command ILIKE '{{ command_pattern }}'
        {% endif %}
        {% if limit -%}
        LIMIT {{ limit }}
        {% endif -%}
        """
    response = execute_query(
        ctx,
        query,
        is_done=is_done,
        command_pattern=command_pattern,
        cluster=cluster,
        limit=limit,
        format_="Vertical",
    )
    logging.info(response)


@mutation_group.command("kill")
@option(
    "--command", "command_pattern", help="Filter mutations to kill by command pattern."
)
@option(
    "--cluster",
    "--on-cluster",
    "on_cluster",
    is_flag=True,
    help="Kill mutations on all hosts of the cluster.",
)
@pass_context
def kill_mutation(ctx, command_pattern, on_cluster):
    """Kill one or several mutations."""
    cluster = get_cluster_name(ctx) if on_cluster else None
    query = """
        KILL MUTATION
        {%- if cluster %}
        ON CLUSTER '{{ cluster }}'
        {%- endif %}
        WHERE NOT is_done
        {% if command_pattern %}
          AND command ILIKE '{{ command_pattern }}'
        {% endif %}
        """
    response = execute_query(
        ctx,
        query,
        command_pattern=command_pattern,
        cluster=cluster,
        echo=True,
        format_="PrettyCompact",
    )
    logging.info(response)
