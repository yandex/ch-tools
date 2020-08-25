from collections import namedtuple

Result = namedtuple('Result', 'code, message')


class Status(object):
    """ Class for holding Juggler status """
    code = 0
    text = []

    def set_code(self, new_code):
        """ Set the code if it is greater than the current. """
        if new_code > self.code:
            self.code = new_code

    def append(self, new_text):
        """Accumulate the status text"""
        self.text.append(new_text)

    def report(self):
        """ Output formatted status message"""
        # concatenate all received statuses
        message = '. '.join(self.text)
        if not message and self.code == 0:
            message = 'Ok'
        # strip underscores and newlines.
        print('%d;%s' % (self.code, message.replace('_', ' ').replace('\n', '').replace('mail.yandex.net', 'm')))
