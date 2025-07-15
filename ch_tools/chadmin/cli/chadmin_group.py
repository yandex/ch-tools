from functools import wraps
from typing import Any, Optional

import click
import cloup

from ch_tools import __version__
from ch_tools.common import logging
from ch_tools.common.utils import get_full_command_name

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
        def wrapper(ctx: Any, *a: Any, **kw: Any) -> None:
            logging.configure(
                ctx.obj["config"]["loguru"],
                "chadmin",
                {"cmd_name": get_full_command_name(ctx)},
            )

            logging.debug(
                "Command starts executing, params: {}, args: {}, version: {}",
                {
                    **ctx.parent.params,
                    **ctx.params,
                },
                ctx.args,
                __version__,
            )

            try:
                cmd_callback(*a, **kw)
                logging.debug("Command completed")
            except Exception:
                logging.exception("Command failed with error:", short_stdout=True)

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
