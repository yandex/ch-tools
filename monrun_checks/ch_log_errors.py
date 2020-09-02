import click
import subprocess

from cloud.mdb.clickhouse.tools.monrun_checks.result import Result


@click.command('log-errors')
@click.option('-c', '--critical', 'crit', type=int, default=60, help='Critical threshold.')
@click.option('-w', '--warning', 'warn', type=int, default=6, help='Warning threshold.')
@click.option('-n', '--watch-seconds', 'watch_seconds', type=int, default=600, help='Watch seconds.')
@click.option('-r', '--regex', 'regex', default='([0-9]{4}\.[0-9]{2}\.[0-9]{2}\ [0-9]{2}\:[0-9]{2}\:[0-9]{2})', help='Regular expresson.')
@click.option('-e', '--exclude', 'exclude', default='e\.displayText\(\) = No message received', help='Excluded error.')
@click.option('-f', '--logfile', 'logfile', default='/var/log/clickhouse-server/clickhouse-server.err.log', help='Log file path.')
def log_errors_command(crit, warn, watch_seconds, regex, exclude, logfile):
    """
    Check errors in clickhouse log.
    """

    tail = subprocess.Popen(['timetail', '-n', f'{watch_seconds}', '-r', f'{regex}', f'{logfile}'], stdout=subprocess.PIPE)
    grep = subprocess.Popen(['grep', '-vP', f'{exclude}'], stdin=tail.stdout, stdout=subprocess.PIPE)
    fgrep = subprocess.Popen(['fgrep', '-c', '<Error>'], stdin=grep.stdout, stdout=subprocess.PIPE)

    errors = int(fgrep.communicate()[0])

    if errors >= crit:
        return Result(2, f'{errors} errors for last {watch_seconds} seconds')
    if errors >= warn:
        return Result(1, f'{errors} errors for last {watch_seconds} seconds')
    return Result(0, f'OK, {errors} errors for last {watch_seconds} seconds')
