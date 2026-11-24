import re

from requests import Response


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """

    def __init__(
        self, query: str, response: Response, *, hide_response: bool = False
    ) -> None:
        self.query = re.sub(r"\s*\n\s*", " ", query.strip())
        self.response = response
        detail = (
            f"HTTP {response.status_code}" if hide_response else response.text.strip()
        )
        super().__init__(f"{detail}\n\nQuery: {self.query}")
