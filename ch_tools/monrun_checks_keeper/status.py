import click
import tabulate

DEFAULT_COLOR = "\033[0m"

COLOR_MAP = {
    0: "\033[92m",
    1: "\033[93m",
    2: "\033[91m",
}


def status_command(commands):
    @click.command("status")
    @click.pass_context
    def status_impl(ctx):
        """
        Perform all checks.
        """
        checks_status = []
        ctx.obj.update({"status_mode": True})
        for cmd in commands:
            status = ctx.invoke(cmd)
            checks_status.append(
                (cmd.name, f"{COLOR_MAP[status.code]}{status.message}{DEFAULT_COLOR}")
            )

        print(tabulate.tabulate(checks_status))

    return status_impl
