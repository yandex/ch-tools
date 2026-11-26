import re
from typing import Optional

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
        if hide_response:
            detail = f"HTTP {response.status_code}"
            code = self.exception_code
            if code is not None:
                detail += f" (ClickHouse code {code})"
        else:
            detail = response.text.strip()
        super().__init__(f"{detail}\n\nQuery: {self.query}")

    @property
    def exception_code(self) -> Optional[int]:
        try:
            return int(self.response.headers.get("X-ClickHouse-Exception-Code", ""))
        except (TypeError, ValueError):
            return None
