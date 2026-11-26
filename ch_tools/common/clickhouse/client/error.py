import re
from typing import Optional

from requests import Response


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """

    def __init__(self, query: str, response: Response) -> None:
        self.query = re.sub(r"\s*\n\s*", " ", query.strip())
        self.response = response
        super().__init__(f"{self.response.text.strip()}\n\nQuery: {self.query}")

    @property
    def code(self) -> Optional[int]:
        """ClickHouse sends the exception code in an HTTP response header."""
        if self.response is None:
            return None
        try:
            return int(self.response.headers.get("X-ClickHouse-Exception-Code", ""))
        except (TypeError, ValueError):
            return None
