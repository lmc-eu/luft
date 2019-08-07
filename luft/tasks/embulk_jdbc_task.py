# -*- coding: utf-8 -*-
"""Embulk JDBC Task."""
import os
from typing import Dict, List

from luft.common.column import Column
from luft.common.config import EMBULK_COMMAND, JDBC_CONFIG, JDBC_DRIVER_PATH, PATH_PREFIX
from luft.common.utils import NoneStr, get_path_prefix, read_config, setup_logger
from luft.tasks.generic_embulk_task import GenericEmbulkTask

# Setup logger
logger = setup_logger('common', 'INFO')


class EmbulkJdbcTask(GenericEmbulkTask):
    """Embulk JDBC Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 columns: List[Column], historize: bool = False, fetch_rows: int = 10000,
                 source_table: NoneStr = None, where_clause: NoneStr = None,
                 embulk_template: NoneStr = None, path_prefix: NoneStr = None,
                 yaml_file: NoneStr = None, env: NoneStr = None, thread_name: NoneStr = None,
                 color: NoneStr = None):
        """Initialize Embulk JDBC Task.

        Attributes:
            name (str): name of task.
            task_type (str): type of task. E.g. embulk-jdbc-load, mongo-load, etc.
            source_system (str): name of source system. Usually name of database.
                Used for better organization especially on blob storage. E.g. jobs, prace, pzr.
            source_subsystem (str): name of source subsystem. Usually name of schema.
                Used for better organization especially on blob storage. E.g. public, b2b.
            env (str): environment - PROD, DEV.
            thread_name(str): name of thread for Airflow parallelization.
            color (str): hex code of color. Airflow operator will have this color.

        """
        self.columns = columns
        self.path_prefix = path_prefix
        self.embulk_template = embulk_template
        self.historize = historize
        self.fetch_rows = fetch_rows
        self.source_table = source_table
        self.where_clause = where_clause
        super().__init__(name=name, task_type=task_type,
                         source_system=source_system,
                         source_subsystem=source_subsystem, yaml_file=yaml_file,
                         env=env, thread_name=thread_name, color=color)

    def __call__(self, ts: str, env: NoneStr = None):
        """Make class callable.

        Attributes:
            ts (str): time of valid.
            env (str): environment.

        """
        cmd = self.get_command()
        args = self.get_command_args()
        env_vars = self.get_env_vars(ts, env)
        logger.info(f'Embulk cmd: {cmd}')
        self._run_subprocess(cmd, args, env_vars)

    def get_command(self) -> List[str]:
        """Get Docker command for running Embulk."""
        return EMBULK_COMMAND

    def get_command_args(self) -> List[str]:
        """Get Docker command arguments for running Embulk."""
        self._set_embulk_template(self.embulk_template)
        return ['run', self.embulk_template, '-l', 'debug']

    def get_env_vars(self, ts: str, env: NoneStr = None) -> Dict[str, str]:
        """Get Docker enviromental variables."""
        super_env_dict = super().get_env_vars(ts=ts, env=env)
        jdbc_params = self._get_jdbc_params()
        blob_storage = self._get_blob_storage_params()
        path_prefix = self._get_path_prefix()
        env_dict = {
            **jdbc_params,
            **blob_storage,
            **path_prefix,
            'SOURCE_TABLE': self._get_source_table(),
            'COLUMNS': self._get_column_list(),
            'COLUMN_OPTIONS': self._get_column_options(),
            'WHERE_CLAUSE': self._get_where_clause()
        }
        clean_dict = self.clean_dictionary(env_dict)
        clean_dict.update(super_env_dict)
        return clean_dict

    def _get_source_table(self) -> str:
        """Get name of source table."""
        table_name = self.source_table.upper() if self.source_table else self.get_name()
        return table_name

    def _get_column_options(self) -> str:
        """Get list of columns for Embulk."""
        col_options = list(map(lambda col: col.get_embulk_column_option(
            filter_ignored=False), self.columns))  # for every column call get_embulk_column_option
        # filter None columns
        return 'column_options:\n    {}'.format('\n    '.join(filter(None, col_options)))

    def _get_column_list(self) -> str:
        """Get column options for Embulk loading."""
        col_list = list(map(lambda col: col.get_aliased_name(
            filter_ignored=False), self.columns))
        return ', '.join(filter(None, col_list))

    def _get_where_clause(self) -> NoneStr:
        """Get where clause."""
        if self.where_clause:
            return self.where_clause.format(date_valid=self.get_date_valid())
        return None

    def _get_path_prefix(self) -> Dict[str, str]:
        path_prefix_ = self.path_prefix or PATH_PREFIX
        path_prefix = {
            'PATH_PREFIX': get_path_prefix(path_prefix=path_prefix_,
                                           env=self.get_env(),
                                           source_system=self.get_source_system(),
                                           source_subsystem=self.get_source_subsystem(),
                                           name=self.get_name(),
                                           date_valid=self.get_date_valid(),
                                           time_valid=self.get_time_valid()
                                           )
        }
        return path_prefix

    def _get_jdbc_params(self) -> Dict[str, str]:
        """Get JDBC parameters."""
        jdbc_config = read_config(JDBC_CONFIG)
        try:
            db = jdbc_config[self.source_system.upper()]
        except KeyError as e:
            raise KeyError('Section %s is missing in jdbc config.' % e)
        try:
            jdbc_params = {
                'TYPE': db['type'],
                'URI': db['uri'],
                'PORT': db['port'],
                'USER': db['user'],
                'PASSWORD': self._get_jdbc_pass(db),
                'DATABASE': db.get('database').format(schema=self.get_source_subsystem().upper()),
                'SCHEMA': self.get_null_param('schema', self.get_source_subsystem()),
                'DRIVER_PATH': JDBC_DRIVER_PATH.get(db['type']),
                'FETCH_ROWS': self.get_null_param('fetch_rows', self.fetch_rows)
            }
            return jdbc_params
        except KeyError as e:
            raise KeyError(
                f'In section {self.source_system.upper()} attrubute {e} is missing in jdbc config.')

    def _get_jdbc_pass(self, db) -> NoneStr:
        """Get JDBC password."""
        if db.get('password') and len(db.get('password')) > 0:
            return db.get('password')
        elif db.get('password_env') and len(db.get('password_env')) > 0:
            return os.getenv(db.get('password_env'))
        else:
            raise ValueError('Missing password for JDBC connector.')
