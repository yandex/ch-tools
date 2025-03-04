from typing import Any, Dict, Optional


class Query:
    mask = "******"

    def __init__(
        self,
        value: str,
        args: Optional[Dict[str, str]] = None,
        sensitive_args: Optional[Dict[str, str]] = None,
    ):
        self.value = value
        self.args = args or {}
        self.sensitive_args = sensitive_args or {}

    def __str__(self) -> str:
        result = self.value
        for key in self.sensitive_args.keys():
            result = result.replace(key, self.mask)
        return result.format(**self.args)

    def __repr__(self) -> str:
        return self.value.format(**{**self.args, **self.sensitive_args})

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))
