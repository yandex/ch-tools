import re


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """

    def __init__(self, query, response):
        self.query = re.sub(r"\s*\n\s*", " ", query.strip())
        self.response = response

        # Extracting clickhouse error code. Pattern like: `... Code: <code>. ...`
        found_code = re.findall(r"Code: \d+", response)[0].split()
        self.code = int(found_code[1]) if len(found_code) == 2 else -1

        super().__init__(
            f"{self.response}\n\nQuery: {self.query}\n\n Cickhouse Error Code: {self.code}"
        )
