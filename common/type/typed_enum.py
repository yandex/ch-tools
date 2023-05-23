"""
Typed enumerations returning their values on `__str__`.
"""

from enum import Enum


class TypedEnum(Enum):
    """
    Base class for typed enumerations.
    """

    def __str__(self) -> str:
        return str(self.value)


class StrEnum(str, TypedEnum):
    """
    String-value enumeration.
    """

    pass


class IntEnum(int, TypedEnum):
    """
    Integer-value enumeration.
    """

    pass
