import logging

from pathlib import Path

import boto3  # type: ignore[import]
import click

from botocore.client import Config  # type: ignore[import]
from click import Context, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage import S3Analyzer, S3AnalyzerStats, S3DiskConfiguration
from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_analyzer_dump_paths import S3AnalyzerDumpPaths
from cloud.mdb.clickhouse.tools.chadmin.internal.utils import human_readable_size

STORAGE_POLICY_CONFIG_PATH = Path('/etc/clickhouse-server/config.d/storage_policy.xml')


@group('object-storage')
@option(
    '-c',
    '--config',
    'config_path',
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
    default=STORAGE_POLICY_CONFIG_PATH,
    help='ClickHouse storage policy config',
    show_default=True,
)
@option(
    '-d',
    '--disk',
    'disk_name',
    default='object_storage',
    help='S3 disk name',
    show_default=True,
)
@pass_context
def object_storage_group(ctx: Context, config_path: Path, disk_name: str) -> None:
    """Commands to manage S3 objects and their metadata."""

    # Restrict excessive boto logging if debug is enabled
    if ctx.obj['debug']:
        _set_boto_log_level(logging.WARNING)

    ctx.obj['disk_configuration'] = S3DiskConfiguration.from_config(config_path, disk_name)


@object_storage_group.command('analyze')
@option(
    '-d',
    '--dump-dir',
    'dump_dir',
    type=click.Path(exists=True, file_okay=False, writable=True, path_type=Path),
    help='dump files directory',
)
@pass_context
def analyze_object_storage(ctx: Context, dump_dir: Path | None = None) -> None:
    disk_conf: S3DiskConfiguration = ctx.obj['disk_configuration']

    s3 = boto3.resource(
        's3',
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
        config=Config(s3={'addressing_style': 'virtual'}),
    )
    bucket = s3.Bucket(disk_conf.bucket_name)
    metadata_paths = _get_disk_metadata_paths(disk_conf.name)
    dump_paths = S3AnalyzerDumpPaths.create(dump_dir) if dump_dir else None

    stats = S3Analyzer(bucket, disk_conf.prefix, dump_paths).analyze(metadata_paths)
    _show_report(stats, dump_paths)


def _set_boto_log_level(level: int) -> None:
    logging.getLogger('boto3').setLevel(level)
    logging.getLogger('botocore').setLevel(level)
    logging.getLogger('nose').setLevel(level)
    logging.getLogger('s3transfer').setLevel(level)
    logging.getLogger('urllib3').setLevel(level)


def _get_disk_metadata_paths(disk_name: str) -> list[Path]:
    return [
        Path(f'/var/lib/clickhouse/disks/{disk_name}/store'),  # Atomic database engine
        Path(f'/var/lib/clickhouse/disks/{disk_name}/data'),  # Ordinary database engine
        Path(f'/var/lib/clickhouse/disks/{disk_name}/shadow'),  # Backups
    ]


def _show_report(stats: S3AnalyzerStats, dump_paths: S3AnalyzerDumpPaths | None) -> None:
    click.echo('*** S3 usage analysis report ***'.center(120))
    column_width = 80

    click.echo(
        'Found metadata for {:,} existing object(s) with total size: {}'.format(
            stats.existing_objects_with_metadata_count,
            human_readable_size(stats.existing_objects_with_metadata_total_size),
        ).ljust(column_width),
        nl=False,
    )
    click.echo('({})'.format(dump_paths.objects_with_metadata) if dump_paths else '')

    click.echo(
        'Not found metadata for {:,} existing object(s) with total size: {} '.format(
            stats.existing_objects_without_metadata_count,
            human_readable_size(stats.existing_objects_without_metadata_total_size),
        ).ljust(column_width),
        nl=False,
    )
    click.echo('({})'.format(dump_paths.objects_without_metadata) if dump_paths else '')

    click.echo(
        'Found metadata for {:,} non-existing object(s) '.format(stats.non_existing_objects_with_metadata_count).ljust(
            column_width
        ),
        nl=False,
    )
    click.echo('({})'.format(dump_paths.objects_not_found) if dump_paths else '')

    click.echo('Unparsed {:,} metadata file(s)'.format(stats.metadata_files_unparsed).ljust(column_width), nl=False)
    click.echo('({})'.format(dump_paths.metadata_files_unparsed) if dump_paths else '')
