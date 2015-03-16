import inspect
import logging


def get_cls_fn_from_bounded_method(method):
    if hasattr(method, 'func'):
        method = method.func
    cls = str(method.im_self.__class__)
    fn = method.__func__.func_name
    return cls, fn


def verbose(msg, logger_name='mdp'):
    logger = logging.getLogger(logger_name)
    if logger.getEffectiveLevel() > logging.DEBUG:
        return
    record = inspect.stack()[2]
    cls = ''
    local = record[0].f_locals

    # class with __call__
    if 'self' in local:
        cls = str(local["self"].__class__)
        fn = record[3]
    # bounded/wrapped method
    else:
        cls, fn = get_cls_fn_from_bounded_method(local['fn'])
    cls = cls[cls.index("'") + 1: cls.rindex("'")]
    output = '{:<40} {:<20} {}'.format(cls, fn, msg)
    logger.debug(output)
    pass
