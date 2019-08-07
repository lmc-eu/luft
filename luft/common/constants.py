# -*- coding: utf-8 -*-
"""Common constants."""
import logging
import sys

# Logging
LOG_LEVEL = 'INFO'
LOG_HANDLER = logging.StreamHandler(stream=sys.stdout)
# If running in Airflow time and level is disturbing
LOG_FORMAT = '%(asctime)-15s [%(levelname)s] %(name)s:%(funcName)s - %(message)s'
