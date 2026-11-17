"""Runtime acceptance checks that remain active under Python optimization."""


def require(condition, message='acceptance check failed'):
    if not condition:
        raise AssertionError(message)
