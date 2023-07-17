from requests import RequestException

from ch_tools.common.result import Status


def user_warning(exc: UserWarning, status: Status) -> Status:
    code, message = exc.args
    status.append(message)
    status.set_code(code)
    return status


def unknown_exception(exc: Exception, status: Status) -> Status:
    status.append(
        "Unknown error: {type}".format(
            type=exc.__class__.__name__,
        )
    )
    status.set_code(1)
    return status


def requests_error(exc: RequestException, status: Status) -> Status:
    status.append(
        "ClickHouse connection error: {type}".format(
            type=exc.__class__.__name__,
        )
    )
    status.set_code(1)
    return status


EXC_MAP = {
    UserWarning: user_warning,
    RequestException: requests_error,
}


def translate_to_status(exc: Exception, status: Status) -> Status:
    handler = unknown_exception
    if exc.__class__ in EXC_MAP:
        handler = EXC_MAP[exc.__class__]  # type: ignore
    return handler(exc, status)


def die(status_code: int, message: str) -> None:
    raise UserWarning(status_code, message)
