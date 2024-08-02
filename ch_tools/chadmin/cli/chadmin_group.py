from functools import wraps
from typing import Optional

import click
import cloup

from ch_tools import __version__
from ch_tools.common import logging

# pylint: disable=too-many-ancestors


class Chadmin(cloup.Group):
    def add_command(
        self,
        cmd: click.Command,
        name: Optional[str] = None,
        section: Optional[cloup.Section] = None,
        fallback_to_default_section: bool = True,
    ) -> None:
        if cmd.callback is None:
            super().add_command(
                cmd,
                name=name,
                section=section,
                fallback_to_default_section=fallback_to_default_section,
            )
            return

        cmd_callback = cmd.callback

        @wraps(cmd_callback)
        @cloup.pass_context
        def wrapper(ctx, *a, **kw):
            logging.configure(
                ctx.obj["config"]["loguru"], "chadmin", {"cmd_name": cmd.name}
            )

            logging.debug(
                "Executing command '{}', params: {}, args: {}, version: {}",
                cmd.name,
                {
                    **ctx.parent.params,
                    **ctx.params,
                },
                ctx.args,
                __version__,
            )

            try:
                cmd_callback(*a, **kw)
                logging.debug("Command '{}' completed", cmd.name)
            except Exception:
                logging.exception("Command '{}' failed with error:", cmd.name)

        cmd.callback = wrapper
        super().add_command(
            cmd,
            name=name,
            section=section,
            fallback_to_default_section=fallback_to_default_section,
        )

    def add_group(
        self,
        cmd: click.Group,
        name: Optional[str] = None,
        section: Optional[cloup.Section] = None,
        fallback_to_default_section: bool = True,
    ) -> None:
        super().add_command(
            cmd,
            name=name,
            section=section,
            fallback_to_default_section=fallback_to_default_section,
        )
