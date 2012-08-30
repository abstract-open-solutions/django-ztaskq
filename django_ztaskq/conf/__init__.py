from .settings import (ZTASKD_LOG_LEVEL, ZTASKD_LOG_PATH, ZTASKD_LOG_MAXBYTES,
                       ZTASKD_LOG_BACKUP)
import logging
from logging.handlers import RotatingFileHandler


def get_logger(name, logfile=ZTASKD_LOG_PATH, loglevel=ZTASKD_LOG_LEVEL):
    LEVELS = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    logger_ = logging.getLogger("ztaskq.%s" % name)
    logger_.propagate = False
    logger_.setLevel(LEVELS[loglevel.lower()])
    if logfile:
        if '%(name)s' in logfile:
            filename = logfile % { 'name': name }
        else:
            filename = logfile
        handler = RotatingFileHandler(filename=filename,
                                      maxBytes=ZTASKD_LOG_MAXBYTES,
                                      backupCount=ZTASKD_LOG_BACKUP)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger_.addHandler(handler)

    return logger_
