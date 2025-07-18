from click import ClickException
from cloup import command


@command("restore-replica")
def restore_replica_command() -> None:
    raise ClickException(
        'The command has been superseded by "replica restore" command.'
    )
