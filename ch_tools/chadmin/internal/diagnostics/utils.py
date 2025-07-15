from functools import partial, wraps
from typing import Any, Callable


def delayed(f: Any) -> Callable:
    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return partial(f, *args, **kwargs)

    return wrapper
