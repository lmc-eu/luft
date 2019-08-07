# -*- coding: utf-8 -*-
"""Utilities for setup logger."""
import logging
import os

from luft.common.constants import (LOG_FORMAT, LOG_HANDLER, LOG_LEVEL)


def setup_logger(log_name, log_level=LOG_LEVEL, log_handler=LOG_HANDLER):
    """Set logging system.

    In case of Airflow it is not good to init logger beause Airflow has
    its own log handlers.

    """
    if not os.getenv('IS_AIRFLOW'):
        logger = logging.getLogger(log_name)
        if not len(logger.handlers):
            log_handler.setFormatter(logging.Formatter(LOG_FORMAT))
            log_handler.setLevel(log_level)
            logger.addHandler(log_handler)
            logger.setLevel(log_level)
            logger.propagate = False
        return logger
    else:
        return logging
