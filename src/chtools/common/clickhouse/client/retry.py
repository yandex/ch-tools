from typing import Any, Tuple, Type, Union

import tenacity


def retry(
    exception_types: Union[Type[BaseException], Tuple[Type[BaseException]]],
    max_attempts: int = 5,
    max_interval: int = 5,
) -> Any:
    """
    Function decorator that retries wrapped function on failures.
    """
    return tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types),
        wait=tenacity.wait_random_exponential(multiplier=0.5, max=max_interval),
        stop=tenacity.stop_after_attempt(max_attempts),
        reraise=True,
    )
