# -*- coding: utf-8 -*-
"""Luft configuration."""
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from luft.common.utils import read_config


class _Config:
    """Config singleton."""

    instance = None
    cfg = None

    def __init__(self, config_file: str):
        self.cfg = read_config(config_file)

    def get_config(self, config_file: str) -> Optional[ConfigParser]:
        return self.cfg


def config(config_file: str) -> ConfigParser:
    """Config singleton."""
    if _Config.instance is None:
        _Config.instance = _Config(config_file)  # noqa: T484
    return _Config.instance.get_config(config_file)  # noqa: T484


# Load .env
load_dotenv('.env')

# Conf
conf = config(os.getenv('LUFT_CONFIG', 'luft.cfg'))

# Core
ENV = os.getenv('ENV', conf.get('core', 'env'))
TASKS_FOLDER = Path(
    os.getenv('TASKS_FOLDER', conf.get('core', 'tasks_folder')))
JDBC_CONFIG = os.getenv('JDBC_CONFIG', conf.get('core', 'jdbc_config'))
BLOB_STORAGE = os.getenv('BLOB_STORAGE', conf.get('core', 'blob_storage'))
PATH_PREFIX = conf.get('core', 'path_prefix')

# BQ
BQ_CREDENTIALS_FILE = os.getenv(
    'BQ_CREDENTIALS_FILE', conf.get('bq', 'credentials_file'))
BQ_PROJECT_ID = os.getenv(
    'BQ_PROJECT_ID', conf.get('bq', 'project_id'))
BQ_LOCATION = os.getenv(
    'BQ_LOCATION', conf.get('bq', 'location'))
BQ_JOB_ID_PREFIX = os.getenv(
    'BQ_JOB_ID_PREFIX', conf.get('bq', 'job_id_prefix'))

# GCS
GCS_BUCKET = os.getenv('GCS_BUCKET', conf.get('gcs', 'bucket'))
GCS_AUTH_METHOD = os.getenv('GCS_AUTH_METHOD', conf.get('gcs', 'auth_method'))
GCS_EMAIL = os.getenv('GCS_EMAIL', conf.get('gcs', 'service_account_email'))
GCS_P12_KEYFILE = os.getenv('GCS_P12_KEYFILE', conf.get('gcs', 'p12_keyfile'))
GCS_JSON_KEYFILE = os.getenv(
    'GCS_JSON_KEYFILE', conf.get('gcs', 'json_keyfile'))
GCS_APP_NAME = os.getenv('GCS_APP_NAME', conf.get(
    'gcs', 'application_name') if conf.get(
    'gcs', 'application_name') != '' else 'embulk-output-gcs')

# AWS
AWS_BUCKET = os.getenv('AWS_BUCKET', conf.get('aws', 'bucket'))
AWS_ENDPOINT = os.getenv('AWS_ENDPOINT', conf.get('aws', 'endpoint'))
AWS_ACCESS_KEY_ID = os.getenv(
    'AWS_ACCESS_KEY_ID', conf.get('aws', 'access_key_id'))
AWS_SECRET_ACCESS_KEY = os.getenv(
    'AWS_SECRET_ACCESS_KEY', conf.get('aws', 'secret_access_key'))

# Task Type Map
TASK_TYPE_MAPPER = conf['task_type_map']

# Embulk
EMBULK_COMMAND = os.getenv('EMBULK_COMMAND', conf.get(
    'embulk', 'embulk_command')).split()

# Embulk Default Template
EMBULK_DEFAULT_TEMPLATE = conf['embulk_default_template']

# Embulk Type map
EMBULK_TYPE_MAPPER = conf['embulk_type_map']

# Thread
DEFAULT_THREAD_CNT = int(conf.get('thread', 'default_thread_cnt'))
THREAD_NAME_PREFIX = conf.get('thread', 'thread_prefix')

# JDBC driver path
JDBC_DRIVER_PATH = conf['jdbc_driver_path']

# Supported data types
SUPPORTED_DATA_TYPES = conf.get('data_type', 'supported_data_types').split()
