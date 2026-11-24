from typing import Any, Dict, Optional


class Query:
    """SQL for display and execution. Equality and hashing use the masked representation."""

    mask = "*****"

    def __init__(
        self,
        value: str,
        sensitive_args: Optional[Dict[str, str]] = None,
        *,
        execution_sql: Optional[str] = None,
    ):
        self._execution_sql = execution_sql
        self.value = value
        self.sensitive_args = sensitive_args or {}

    def for_execute(self) -> str:
        return (
            self._execution_sql
            if self._execution_sql is not None
            else self._render(False)
        )

    @property
    def contains_secrets(self) -> bool:
        return bool(self.sensitive_args) or self._execution_sql is not None

    def _render(self, mask_sensitive: bool = True) -> str:
        if not self.sensitive_args:
            return self.value
        sensitive_args = (
            self._sensitive_args_mask() if mask_sensitive else self.sensitive_args
        )
        return self.value.format(**sensitive_args)

    def _sensitive_args_mask(self) -> Dict[str, str]:
        return {key: self.mask for key in self.sensitive_args}

    def __str__(self) -> str:
        return self._render(True)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(value='{str(self)}', sensitive_args={self._sensitive_args_mask()})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))

    def __add__(self, other: str) -> "Query":
        return Query(
            self.value + other,
            self.sensitive_args,
            execution_sql=(
                self._execution_sql + other if self._execution_sql is not None else None
            ),
        )
