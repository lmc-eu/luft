# -*- coding: utf-8 -*-
"""Common utils."""
import importlib
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Optional, Union

import dateutil.parser

from luft.common.logger import setup_logger

import pendulum


# Setup logger
logger = setup_logger('util', 'INFO')

NoneStr = Union[Optional[str], None]


def class_for_name(path: str) -> Any:
    """Return the value of the named attribute of object."""
    parts = path.split('.')
    class_name = parts.pop()
    module_name = '.'.join(parts)
    m = importlib.import_module(module_name)
    c = getattr(m, class_name)
    return c


def ts_to_tz(ts: str) -> str:
    """Convert timestamp to timezone aware timestamp."""
    local_tz = pendulum.timezone(os.getenv('DEFAULT_TIMEZONE', 'UTC'))
    tz = dateutil.parser.parse(ts)
    ts_loc = local_tz.convert(tz)
    return ts_loc


def get_path_prefix(path_prefix: str, env: NoneStr = None,
                    source_system: NoneStr = None, source_subsystem: NoneStr = None,
                    name: NoneStr = None, date_valid: NoneStr = None,
                    time_valid: NoneStr = None):
    """Format path_prefix."""
    path = path_prefix.format(env=env,
                              source_system=source_system,
                              source_subsystem=source_subsystem,
                              name=name,
                              date_valid=date_valid,
                              time_valid=time_valid
                              )
    return path


def get_source_system(path: str = None, source_system: str = None) -> Optional[str]:
    """Get source system name from variable or path."""
    if source_system:
        return source_system
    elif path:
        return _get_path_part(path, 'source_system')
    return None


def get_source_subsystem(path: str = None, source_subsystem: str = None) -> Optional[str]:
    """Get source system name from variable or path."""
    if source_subsystem:
        return source_subsystem.lower()
    elif path:
        return _get_path_part(path, 'source_subsystem')
    return None


def _get_path_part(path: str, part: str) -> Optional[str]:
    """Get source_system, source_subsystem and object_name from path."""
    part_mapper = {
        'source_system': -3,
        'source_subsystem': -2,
        'object_name': -1
    }
    path_object = Path(path)
    if path_object.exists():
        position = part_mapper[part] + \
            1 if path_object.is_dir() else part_mapper[part]
        result = path_object.parts[position]
        logger.debug('Setting %s: %s', part, result)
        return result
    else:
        logger.info('Path %s you provided does not exists', path)
        return None


def read_config(config_file: str) -> ConfigParser:
    """Read configuration."""
    if Path(config_file).exists():
        cfg = ConfigParser()
        cfg.read(config_file)
        return cfg
    else:
        raise FileNotFoundError(
            'Config file `%s` does not exists.' % config_file)
