from click import group, option, pass_context
from cloud.mdb.cli.common.formatting import format_bytes, print_response

from cloud.mdb.clickhouse.tools.chadmin.cli import get_cluster_name
from cloud.mdb.clickhouse.tools.chadmin.internal.process import list_merges

FIELD_FORMATTERS = {
    'total_size_bytes_compressed': format_bytes,
    'bytes_read_uncompressed': format_bytes,
    'bytes_written_uncompressed': format_bytes,
    'memory_usage': format_bytes,
}


@group('merge')
def merge_group():
    """Commands to manage merges (from system.merges)."""
    pass


@merge_group.command(name='list')
@option('--cluster', '--on-cluster', 'on_cluster', is_flag=True, help='Get merges from all hosts in the cluster.')
@option('-l', '--limit', type=int, default=1000, help='Limit the max number of objects in the output.')
@pass_context
def list_command(ctx, on_cluster, limit):
    """List executing merges."""
    cluster = get_cluster_name(ctx) if on_cluster else None

    merges = list_merges(ctx, cluster=cluster, limit=limit)

    print_response(ctx, merges, default_format='yaml', field_formatters=FIELD_FORMATTERS)
