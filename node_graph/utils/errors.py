import logging


class MyException(Exception):
    pass


class InvalidInputException(MyException):
    pass


class NetworkException(MyException):
    pass


class TraversalGraphException(MyException):
    pass


class InvalidInitializationDB(MyException):
    pass


class InvalidInsertItemDB(MyException):
    pass


def throw_error(msg, exception=MyException):
    logging.error(msg)
    if THROW_EXCEPTION_ON_ERROR:
        raise exception(msg)


def set_throw_on_error(throw=True):
    global THROW_EXCEPTION_ON_ERROR
    THROW_EXCEPTION_ON_ERROR = throw

