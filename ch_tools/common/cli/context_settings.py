from cloup import Context, HelpFormatter, HelpTheme

__all__ = [
    "CONTEXT_SETTINGS",
]

CONTEXT_SETTINGS = Context.settings(
    help_option_names=["-h", "--help"],
    terminal_width=120,
    align_option_groups=False,
    align_sections=True,
    show_constraints=True,
    formatter_settings=HelpFormatter.settings(
        theme=HelpTheme.light(),
    ),
)
