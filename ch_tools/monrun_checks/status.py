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
        config = ctx.obj["config"]["ch-monitoring"]
        ctx.obj["status_mode"] = True
        ctx.default_map = config

        checks_status = []
        for cmd in commands:
            if not config.get(cmd.name, {}).get("@disabled"):
                status = ctx.invoke(cmd)
                checks_status.append(
                    (
                        cmd.name,
                        f"{COLOR_MAP[status.code]}{status.message}{DEFAULT_COLOR}",
                    )
                )

        print(tabulate.tabulate(checks_status))

    return status_impl
