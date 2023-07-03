import re


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """

    def __init__(self, query, response):
        self.query = re.sub(r"\s*\n\s*", " ", query.strip())
        self.response = response
        super().__init__(f"{self.response.text.strip()}\n\nQuery: {self.query}")
