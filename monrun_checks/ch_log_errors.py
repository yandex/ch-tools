import click
import subprocess
import re

from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


default_regex = r'([0-9]{4}\.[0-9]{2}\.[0-9]{2}\ [0-9]{2}\:[0-9]{2}\:[0-9]{2})'
default_exclude = r'e\.displayText\(\) = No message received'


def validate_regex(ctx, param, value):
    if not value:
        return default_regex
    try:
        re.compile(value)
    except re.error:
        raise click.BadParameter('Value should be a valid regular expression.')
    return value


def validate_exclude(ctx, param, value):
    if not value:
        return default_exclude
    try:
        re.compile(value)
    except re.error:
        raise click.BadParameter('Value should be a valid regular expression.')
    return value


@click.command('log-errors')
@click.option('-c', '--critical', 'crit', type=int, default=60, help='Critical threshold.')
@click.option('-w', '--warning', 'warn', type=int, default=6, help='Warning threshold.')
@click.option('-n', '--watch-seconds', 'watch_seconds', type=int, default=600, help='Watch seconds.')
@click.option('-r', '--regex', 'regex', default=default_regex, callback=validate_regex, help='Regular expresson.')
@click.option('-e', '--exclude', 'exclude', default=default_exclude, callback=validate_exclude, help='Excluded error.')
@click.option(
    '-f', '--logfile', 'logfile', default='/var/log/clickhouse-server/clickhouse-server.err.log', help='Log file path.'
)
def log_errors_command(crit, warn, watch_seconds, regex, exclude, logfile):
    """
    Check errors in clickhouse log.
    """

    tail = subprocess.Popen(
        ['timetail', '-n', f'{watch_seconds}', '-r', f'{regex}', f'{logfile}'],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    errors = 0

    while True:
        line = tail.stdout.readline()
        if not line:
            break
        line_str = line.decode("utf-8")
        if not re.search(exclude, line_str) and re.search('<Error>', line_str):
            errors += 1

    if errors >= crit:
        return Result(2, f'{errors} errors for last {watch_seconds} seconds')
    if errors >= warn:
        return Result(1, f'{errors} errors for last {watch_seconds} seconds')
    return Result(0, f'OK, {errors} errors for last {watch_seconds} seconds')
