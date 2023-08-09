from click import argument, group, option, pass_context

from ch_tools.chadmin.cli import get_cluster_name
from ch_tools.chadmin.internal.utils import execute_query


@group("mutation")
def mutation_group():
    """
    Commands to manage mutations.
    """
    pass


@mutation_group.command(name="get")
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
    print(execute_query(ctx, query, mutation=mutation, format_="Vertical"))


@mutation_group.command(name="list")
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
@pass_context
def list_mutations(ctx, is_done, command_pattern, on_cluster):
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
        """
    response = execute_query(
        ctx,
        query,
        is_done=is_done,
        command_pattern=command_pattern,
        cluster=cluster,
        format_="Vertical",
    )
    print(response)


@mutation_group.command(name="kill")
@pass_context
def kill_mutation(ctx):
    """Kill one or several mutations."""
    query = """
        KILL MUTATION
        WHERE NOT is_done
        """
    print(execute_query(ctx, query, format_="PrettyCompact"))
