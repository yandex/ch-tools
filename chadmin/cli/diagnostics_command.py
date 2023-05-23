import cloup

from chadmin.internal.diagnostics.diagnose import diagnose
from common.cli.parameters import env_var_help


@cloup.command('diagnostics', help='Perform diagnostics on ClickHouse.')
@cloup.option(
    '-o',
    '--format',
    'output_format',
    type=cloup.Choice(
        choices=['json', 'yaml', 'json.gz', 'yaml.gz', 'wiki', 'wiki.gz'],
        case_sensitive=False,
    ),
    default='wiki',
    envvar='CHADMIN_DIAGNOSTICS_FORMAT',
    help='Output format for gathered diagnostics data. ' + env_var_help('CHADMIN_DIAGNOSTICS_FORMAT'),
)
@cloup.option(
    '-n',
    '--normalize-queries',
    is_flag=True,
    envvar='CHADMIN_DIAGNOSTICS_NORMALIZE_QUERIES',
    help='Whether to normalize queries for ClickHouse client. ' + env_var_help('CHADMIN_DIAGNOSTICS_NORMALIZE_QUERIES'),
)
def diagnostics_command(output_format: str, normalize_queries: bool):
    diagnose(output_format, normalize_queries)
