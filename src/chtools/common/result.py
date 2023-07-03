class Result:
    def __init__(self, code=0, message="Ok", verbose=""):
        self.code = code
        self.message = message
        self.verbose = verbose


class Status:
    """Class for holding Juggler status."""

    def __init__(self):
        self.code = 0
        self.text = []
        self.verbose = []

    @property
    def message(self):
        """Result message."""
        # concatenate all received statuses
        message = ". ".join(self.text)
        if not message and self.code == 0:
            message = "OK"

        # strip underscores and newlines.
        message = message.replace("_", " ").replace("\n", "")
        # this is for prometheus labels, see thread:
        # https://groups.google.com/g/prometheus-users/c/kWxGNfd4dlE/m/veNLgMCLAgAJ
        message = message.replace('"', "'")
        return message

    def set_code(self, new_code):
        """Set the code if it is greater than the current."""
        if new_code > self.code:
            self.code = new_code

    def append(self, new_text):
        """Accumulate the status text."""
        self.text.append(new_text)

    def add_verbose(self, new_text):
        """Add detail info."""
        self.verbose.append(new_text)

    def report(self):
        """Output formatted status message."""
        print(f"{self.code};{self.message}")
        if self.verbose:
            for v in self.verbose:
                if v:
                    print("\n")
                    print(v)
