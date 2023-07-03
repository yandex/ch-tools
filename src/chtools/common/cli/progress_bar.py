from typing import Any, Generator, Sequence, TypeVar, Union

from tqdm import tqdm

__all__ = ["progress"]

T = TypeVar("T")


def progress(
    i: Sequence[Union[T, Any]], description: str
) -> Generator[Union[T, Any], None, None]:
    for e in tqdm(i, desc=description, colour="green"):
        yield e
