from typing import Any, Dict, Optional


class Query:
    mask = "*****"

    def __init__(
        self,
        value: str,
        sensitive_args: Optional[Dict[str, str]] = None,
    ):
        self.value = value
        self.sensitive_args = sensitive_args or {}

    def for_execute(self) -> str:
        return self._render(False)

    def _render(self, mask_sensitive: bool = True) -> str:
        if not self.sensitive_args:
            return self.value
        if mask_sensitive:
            sensitive_args = {key: self.mask for key in self.sensitive_args}
        else:
            sensitive_args = self.sensitive_args
        return self.value.format(**sensitive_args)

    def __str__(self) -> str:
        return self._render(True)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(value={self.value!r}, sensitive_args={self.sensitive_args!r})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))

    def __add__(self, other: str) -> "Query":
        return Query(self.value + other, self.sensitive_args)
