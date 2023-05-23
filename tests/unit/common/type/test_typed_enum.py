from functools import reduce
from typing import Sequence, TypeVar

from hamcrest import assert_that, equal_to
from pytest import mark

from common.type.typed_enum import IntEnum, StrEnum, TypedEnum

T = TypeVar('T', int, str)


class SEnum(StrEnum):
    A = 'AAA'
    B = 'BBB'


class IEnum(IntEnum):
    A = 1
    B = 2


@mark.parametrize(
    ['inputs', 'stringified_expected', 'summed_expected'],
    [
        ((SEnum.A,), ['AAA'], 'AAA'),
        ((SEnum.B,), ['BBB'], 'BBB'),
        ((SEnum.A, SEnum.A), ['AAA', 'AAA'], 'AAAAAA'),
        ((SEnum.A, SEnum.B), ['AAA', 'BBB'], 'AAABBB'),
        ((SEnum.B, SEnum.A), ['BBB', 'AAA'], 'BBBAAA'),
        ((SEnum.B, SEnum.B), ['BBB', 'BBB'], 'BBBBBB'),
        ((SEnum.B, SEnum.B, SEnum.B), ['BBB', 'BBB', 'BBB'], 'BBBBBBBBB'),
        ((IEnum.A,), ['1'], 1),
        ((IEnum.B,), ['2'], 2),
        ((IEnum.A, IEnum.A), ['1', '1'], 2),
        ((IEnum.A, IEnum.B), ['1', '2'], 3),
        ((IEnum.B, IEnum.A), ['2', '1'], 3),
        ((IEnum.B, IEnum.B), ['2', '2'], 4),
        ((IEnum.B, IEnum.B, IEnum.B), ['2', '2', '2'], 6),
    ],
)
def test_typed_enum(inputs: Sequence[TypedEnum], stringified_expected: Sequence[str], summed_expected: T):
    stringified: Sequence[str] = [str(i) for i in inputs]
    assert_that(stringified, equal_to(stringified_expected))

    summed = reduce(lambda a, b: a + b, inputs)
    assert_that(summed, equal_to(summed_expected))
