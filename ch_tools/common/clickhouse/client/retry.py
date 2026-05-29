from http import HTTPStatus
from typing import Any, Optional, Tuple, Type, Union

import requests
import tenacity

from ch_tools.common import logging

from .error import ClickhouseError

RETRYABLE_CLICKHOUSE_ERROR_CODES = {
    999,  # KEEPER_EXCEPTION
}

RETRYABLE_HTTP_STATUS_CODES = {
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
}


def _get_clickhouse_error_code(exc: ClickhouseError) -> Optional[int]:
    """
    Extract ClickHouse exception code from the response.

    ClickHouse sets the X-ClickHouse-Exception-Code header in HTTP 500
    responses when the error occurs before streaming starts.
    """
    if exc.response is None:
        return None
    header_value = exc.response.headers.get("X-ClickHouse-Exception-Code", "")
    try:
        return int(header_value)
    except (TypeError, ValueError):
        return None


def is_transient_error(exc: BaseException) -> bool:
    """
    Determine if an error is transient and can be retried.
    """
    # Network-related errors are retryable
    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True

    # ClickHouse HTTP errors - check status code. Do not rely on
    # requests.Response truthiness: 4xx/5xx responses are falsy.
    if isinstance(exc, ClickhouseError):
        if exc.response is None:
            return False
        if exc.response.status_code in RETRYABLE_HTTP_STATUS_CODES:
            return True
        return _get_clickhouse_error_code(exc) in RETRYABLE_CLICKHOUSE_ERROR_CODES

    return False


def retry(
    exception_types: Union[Type[BaseException], Tuple[Type[BaseException], ...]],
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


def _log_retry_attempt(retry_state: tenacity.RetryCallState) -> None:
    """Log a warning before each retry sleep."""
    if retry_state.outcome is None or retry_state.next_action is None:
        return
    exc = retry_state.outcome.exception()
    logging.warning(
        f"Query failed (attempt {retry_state.attempt_number}): {exc!r}. "
        f"Retrying in {retry_state.next_action.sleep:.2f}s..."
    )
