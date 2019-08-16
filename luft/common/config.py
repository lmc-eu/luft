# -*- coding: utf-8 -*-
"""Luft configuration."""
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from luft.common.logger import setup_logger
from luft.common.utils import read_config

# Setup logger
logger = setup_logger('common', 'INFO')


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


def get_cfg(section, name, default=None):
    """Get configuration."""
    if conf.has_option(section, name):
        return conf.get(section, name)
    else:
        return default


# Core
ENV = os.getenv('ENV', get_cfg('core', 'env'))
TASKS_FOLDER = Path(
    os.getenv('TASKS_FOLDER', get_cfg('core', 'tasks_folder')))
JDBC_CONFIG = os.getenv('JDBC_CONFIG', get_cfg('core', 'jdbc_config'))
BLOB_STORAGE = os.getenv('BLOB_STORAGE', get_cfg('core', 'blob_storage'))
PATH_PREFIX = get_cfg('core', 'path_prefix')

# BQ
BQ_CREDENTIALS_FILE = os.getenv(
    'BQ_CREDENTIALS_FILE', get_cfg('bq', 'credentials_file'))
BQ_PROJECT_ID = os.getenv(
    'BQ_PROJECT_ID', get_cfg('bq', 'project_id'))
BQ_LOCATION = os.getenv(
    'BQ_LOCATION', get_cfg('bq', 'location'))
BQ_JOB_ID_PREFIX = os.getenv(
    'BQ_JOB_ID_PREFIX', get_cfg('bq', 'job_id_prefix'))

# GCS
GCS_BUCKET = os.getenv('GCS_BUCKET', get_cfg('gcs', 'bucket'))
GCS_AUTH_METHOD = os.getenv(
    'GCS_AUTH_METHOD', get_cfg('gcs', 'auth_method'))
GCS_EMAIL = os.getenv('GCS_EMAIL', get_cfg(
    'gcs', 'service_account_email'))
GCS_P12_KEYFILE = os.getenv(
    'GCS_P12_KEYFILE', get_cfg('gcs', 'p12_keyfile'))
GCS_JSON_KEYFILE = os.getenv(
    'GCS_JSON_KEYFILE', get_cfg('gcs', 'json_keyfile'))
GCS_APP_NAME = os.getenv('GCS_APP_NAME', get_cfg(
    'gcs', 'application_name') if get_cfg(
    'gcs', 'application_name') != '' else 'embulk-output-gcs')

# AWS
AWS_BUCKET = os.getenv('AWS_BUCKET', get_cfg('aws', 'bucket'))
AWS_ENDPOINT = os.getenv('AWS_ENDPOINT', get_cfg('aws', 'endpoint'))
AWS_ACCESS_KEY_ID = os.getenv(
    'AWS_ACCESS_KEY_ID', get_cfg('aws', 'access_key_id'))
AWS_SECRET_ACCESS_KEY = os.getenv(
    'AWS_SECRET_ACCESS_KEY', get_cfg('aws', 'secret_access_key'))

# Qlik Cloud
QLIK_CLOUD_DELAY = get_cfg('qlik_cloud', 'delay')
QLIK_CLOUD_LOGIN_URL = get_cfg('qlik_cloud', 'login_url')
QLIK_CLOUD_URL = get_cfg('qlik_cloud', 'url')
QLIK_CLOUD_USER = os.getenv(
    'QLIK_CLOUD_USER', get_cfg('qlik_cloud', 'user'))
QLIK_CLOUD_PASS = os.getenv(
    'QLIK_CLOUD_PASS', get_cfg('qlik_cloud', 'password'))
QLIK_CLOUD_HEADLESS = False if get_cfg(
    'qlik_cloud', 'headless').lower() == 'false' else True

# Qlik Enterprise Server
QLIK_ENT_HOST = os.getenv(
    'QLIK_ENT_HOST', get_cfg('qlik_enterprise', 'host'))
QLIK_ENT_PORT = os.getenv(
    'QLIK_ENT_PORT', get_cfg('qlik_enterprise', 'port'))
QLIK_ENT_CLIENT_KEY = os.getenv(
    'QLIK_ENT_CLIENT_KEY', get_cfg('qlik_enterprise', 'client_key'))
QLIK_ENT_ROOT_CERT = os.getenv(
    'QLIK_ENT_ROOT_CERT', get_cfg('qlik_enterprise', 'root_cert'))
QLIK_ENT_CLIENT_CERT = os.getenv(
    'QLIK_ENT_CLIENT_CERT', get_cfg('qlik_enterprise', 'client_cert'))


# Task Type Map
TASK_TYPE_MAPPER = conf['task_type_map']

# Embulk
EMBULK_COMMAND = os.getenv('EMBULK_COMMAND', get_cfg(
    'embulk', 'embulk_command')).split()

# Embulk Default Template
EMBULK_DEFAULT_TEMPLATE = conf['embulk_default_template']

# Embulk Type map
EMBULK_TYPE_MAPPER = conf['embulk_type_map']

# Thread
DEFAULT_THREAD_CNT = int(get_cfg('thread', 'default_thread_cnt'))
THREAD_NAME_PREFIX = get_cfg('thread', 'thread_prefix')

# JDBC driver path
JDBC_DRIVER_PATH = conf['jdbc_driver_path']

# Supported data types
SUPPORTED_DATA_TYPES = get_cfg(
    'data_type', 'supported_data_types').split()
