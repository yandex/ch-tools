import re

from click import Context

OK = 0
WARNING = 1
CRIT = 2


class Result:
    def __init__(self, code: int = OK, message: str = "OK", verbose: str = "") -> None:
        self.code = code
        self.message = message
        self.verbose = verbose


class Status:
    """Class for holding Juggler status."""

    def __init__(self) -> None:
        self.code = 0
        self.text: list[str] = []
        self.verbose: list[str] = []

    @property
    def message(self) -> str:
        """Result message."""
        # concatenate all received statuses
        message = ". ".join(self.text)
        if not message and self.code == 0:
            message = "OK"

        return message

    def set_code(self, new_code: int) -> None:
        """Set the code if it is greater than the current."""
        if new_code > self.code:
            self.code = new_code

    def append(self, new_text: str) -> None:
        """Accumulate the status text."""
        self.text.append(new_text)

    def add_verbose(self, new_text: str) -> None:
        """Add detail info."""
        self.verbose.append(new_text)

    def report(self, ctx: Context) -> None:
        """Output formatted status message."""
        message = self.message
        for rule in ctx.obj["config"]["monitoring"]["output"]["escaping_rules"]:
            message = re.sub(rule["pattern"], rule["replacement"], message)

        print(f"{self.code};{message}")
        if self.verbose:
            for v in self.verbose:
                if v:
                    print("\n")
                    print(v)
