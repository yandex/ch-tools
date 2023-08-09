from functools import partial, wraps


def delayed(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return partial(f, *args, **kwargs)

    return wrapper
