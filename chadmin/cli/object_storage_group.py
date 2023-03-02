import json
import logging

from dataclasses import asdict
from pathlib import Path

import boto3  # type: ignore[import]
import click
import yaml

from botocore.client import Config  # type: ignore[import]
from click import Context, group, option, pass_context

from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage import S3Analyzer, S3AnalyzerStats, S3DiskConfiguration
from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.dump_writer import FileDumpWriter, NullDumpWriter
from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_analyzer import CommonStats
from cloud.mdb.clickhouse.tools.chadmin.internal.object_storage.s3_cleaner import S3Cleaner
from cloud.mdb.internal.python.cli.formatting import format_bytes

STORAGE_POLICY_CONFIG_PATH = Path('/etc/clickhouse-server/config.d/storage_policy.xml')
DEFAULT_DUMP_DIR = Path('/tmp')


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

    # Restrict excessive boto logging
    _set_boto_log_level(logging.WARNING)

    ctx.obj['disk_configuration'] = S3DiskConfiguration.from_config(config_path, disk_name)


@object_storage_group.command('analyze')
@option(
    '-d',
    '--dump',
    'dump_dir',
    is_flag=False,
    flag_value=DEFAULT_DUMP_DIR,
    type=click.Path(exists=True, file_okay=False, writable=True, path_type=Path),
    metavar='[DIRECTORY]',
    help='Dump files directory. If the option has no value, the `/tmp` default is used',
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

    if dump_dir:
        dump_writer = FileDumpWriter(dump_dir)
    else:
        dump_writer = NullDumpWriter()

    with dump_writer:
        stats = S3Analyzer(bucket, dump_writer, disk_conf.prefix).analyze(metadata_paths)

    match ctx.obj['format']:
        case 'json':
            click.echo(json.dumps(asdict(stats)))
        case 'yaml':
            click.echo(yaml.dump(asdict(stats)))
        case _:
            _show_human_friendly_report(disk_conf.bucket_name, stats)


@object_storage_group.command('clean')
@option(
    '-f',
    '--dump-file',
    'dump_path',
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path),
    help='Dump file produced by analyze command',
    required=True,
)
@pass_context
def clean_object_storage(ctx: Context, dump_path: Path) -> None:
    disk_conf: S3DiskConfiguration = ctx.obj['disk_configuration']

    s3 = boto3.resource(
        's3',
        endpoint_url=disk_conf.endpoint_url,
        aws_access_key_id=disk_conf.access_key_id,
        aws_secret_access_key=disk_conf.secret_access_key,
        config=Config(s3={'addressing_style': 'virtual'}),
    )
    bucket = s3.Bucket(disk_conf.bucket_name)

    deleted = S3Cleaner(bucket).clean_dumped_objects(dump_path)
    click.echo(f'Deleted {deleted} objects from bucket [{bucket.name}]')


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


def _show_human_friendly_report(bucket_name: str, stats: S3AnalyzerStats) -> None:
    click.echo('*** S3 usage analysis report ***'.center(120))
    click.echo('Bucket name: {}'.format(bucket_name))
    click.echo(_make_report_line('Objects with metadata', stats.objects_with_metadata))
    click.echo(_make_report_line('Objects without metadata', stats.objects_without_metadata))
    click.echo(_make_report_line('Ignored objects', stats.ignored_objects))
    click.echo(_make_report_line('Not-found objects with metadata', stats.not_found_objects_with_metadata))
    click.echo(_make_report_line('Unparsed metadata files', stats.unparsed_metadata))


def _make_report_line(title: str, stats: CommonStats) -> str:
    stats_line = '{:45} count: {:<12,} total_size: {:<14}'.format(title, stats.count, format_bytes(stats.total_size))
    return '{:<90} {}'.format(stats_line, 'Dump: {}'.format(stats.dump_path) if stats.dump_path else '')
