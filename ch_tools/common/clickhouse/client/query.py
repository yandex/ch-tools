from typing import Any, Callable, Dict, Optional


class Query:
    """SQL for display and execution. Equality and hashing use the masked representation."""

    mask = "*****"

    def __init__(
        self,
        value: str | Callable[[bool], str],
        sensitive_args: Optional[Dict[str, str]] = None,
    ):
        self._value = value
        self.sensitive_args = sensitive_args or {}

    @property
    def value(self) -> str:
        return self._value(True) if callable(self._value) else self._value

    @property
    def display_sql(self) -> str:
        return self._render(True)

    @property
    def execution_sql(self) -> str:
        return self._render(False)

    def for_execute(self) -> str:
        return self.execution_sql

    @property
    def contains_secrets(self) -> bool:
        return bool(self.sensitive_args) or callable(self._value)

    def _render(self, mask_sensitive: bool) -> str:
        text = self._value(mask_sensitive) if callable(self._value) else self._value
        if not self.sensitive_args:
            return text
        args = self._sensitive_args_mask() if mask_sensitive else self.sensitive_args
        return text.format(**args)

    def _sensitive_args_mask(self) -> Dict[str, str]:
        return {key: self.mask for key in self.sensitive_args}

    def __str__(self) -> str:
        return self.display_sql

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(value='{str(self)}', sensitive_args={self._sensitive_args_mask()})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))

    def __add__(self, other: str) -> "Query":
        if callable(self._value):
            return Query(lambda masked: self._render(masked) + other)
        return Query(self._value + other, self.sensitive_args)
