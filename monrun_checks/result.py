from collections import namedtuple

Result = namedtuple('Result', 'code, message')


class Status:
    """Class for holding Juggler status."""

    def __init__(self):
        self.code = 0
        self.text = []

    @property
    def message(self):
        """Result message."""
        # concatenate all received statuses
        message = '. '.join(self.text)
        if not message and self.code == 0:
            message = 'OK'

        # strip underscores and newlines.
        return message.replace('_', ' ').replace('\n', '')

    def set_code(self, new_code):
        """Set the code if it is greater than the current."""
        if new_code > self.code:
            self.code = new_code

    def append(self, new_text):
        """Accumulate the status text."""
        self.text.append(new_text)

    def report(self):
        """Output formatted status message."""
        print(f'{self.code};{self.message}')
