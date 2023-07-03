import cloup

from chtools.chadmin.internal.diagnostics.diagnose import diagnose
from click import pass_context
from chtools.common.cli.parameters import env_var_help


@cloup.command("diagnostics")
@cloup.option(
    "-o",
    "--format",
    "output_format",
    type=cloup.Choice(
        choices=["json", "yaml", "json.gz", "yaml.gz", "wiki", "wiki.gz"],
        case_sensitive=False,
    ),
    default="wiki",
    envvar="CHADMIN_DIAGNOSTICS_FORMAT",
    help="Output format for gathered diagnostics data. "
    + env_var_help("CHADMIN_DIAGNOSTICS_FORMAT"),
)
@cloup.option(
    "-n",
    "--normalize-queries",
    is_flag=True,
    envvar="CHADMIN_DIAGNOSTICS_NORMALIZE_QUERIES",
    help="Whether to normalize queries for ClickHouse client. "
    + env_var_help("CHADMIN_DIAGNOSTICS_NORMALIZE_QUERIES"),
)
@pass_context
def diagnostics_command(ctx, output_format: str, normalize_queries: bool):
    """
    Collect diagnostics data.
    """
    diagnose(ctx, output_format, normalize_queries)
