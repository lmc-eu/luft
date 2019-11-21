# -*- coding: utf-8 -*-
"""Embulk JDBC Task."""
import os
from typing import Dict, List

from luft.common.column import Column
from luft.common.config import (
    EMBULK_COMMAND, EMBULK_LOG_LEVEL, ES_CONFIG, PATH_PREFIX)
from luft.common.utils import NoneStr, get_path_prefix, read_config, setup_logger
from luft.tasks.generic_embulk_task import GenericEmbulkTask

import dateutil.parser


# Setup logger
logger = setup_logger('common', 'INFO')

class EmbulkEsTask(GenericEmbulkTask):
    """Embulk Elasticsearch Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 columns: List[Column],
                 index_name: NoneStr = None, type_name: NoneStr = None, queries_clause: NoneStr = None,
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
        self.index_name = index_name
        self.type_name = type_name
        self.queries_clause = queries_clause
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
        return ['run', self.embulk_template, '-l', EMBULK_LOG_LEVEL]

    def get_env_vars(self, ts: str, env: NoneStr = None) -> Dict[str, str]:
        """Get Docker enviromental variables."""
        super_env_dict = super().get_env_vars(ts=ts, env=env)
        es_params = self._get_es_params()
        blob_storage = self._get_blob_storage_params()
        path_prefix = self._get_path_prefix()

        env_dict = {
            **es_params,
            **blob_storage,
            **path_prefix,
            'ES_FIELDS': self._get_column_list(),
            'ES_QUERIES': self._get_queries_clause(),
            'ES_INDEX': self._get_index_name(),
            'ES_INDEX_TYPE': self.type_name,
        }
        clean_dict = self.clean_dictionary(env_dict)
        clean_dict.update(super_env_dict)
        return clean_dict

    def _get_queries_clause(self) -> str:
        """Get queries clause."""
        q = ''
        if self.queries_clause:
            q = self.queries_clause.format(date_valid=self.get_date_valid())

        return f'  queries:\n   - {q}'

    def _get_column_list(self) -> str:
        """Get column options for Embulk loading."""
        col_list = []
        for col in self.columns:
            metadata = ""
            if (col.metadata):
                metadata = f", metadata: {col.metadata}"
            str = f"\n   - {{ name: {col.name}, type: {col._embulk_column_mapper()} {metadata}}}"
            col_list.append(str)

        return '  fields:{}'.format(''.join(filter(None, col_list)))


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

    def _get_es_params(self) -> Dict[str, str]:
        """Get JDBC parameters."""
        es_conf = read_config(ES_CONFIG)
        try:
            es = es_conf[self.source_system.upper()]
        except KeyError as e:
            raise KeyError('Section %s is missing in es config.' % e)
        try:
            es_params = {
                'ES_URI': es['uri'],
                'ES_PORT': es['port'],
                'ES_USER': es['user'],
                'ES_PASSWORD': self._get_es_pass(es),
                'ES_SCHEME': es['scheme'],
            }
            return es_params
        except KeyError as e:
            raise KeyError(
                f'In section {self.source_system.upper()} attrubute {e} is missing in jdbc config.')

    def _get_es_pass(self, db) -> NoneStr:
        """Get JDBC password."""
        if db.get('password') and len(db.get('password')) > 0:
            return db.get('password')
        elif db.get('password_env') and len(db.get('password_env')) > 0:
            return os.getenv(db.get('password_env'))
        else:
            raise ValueError('Missing password for ES connector.')

    def _get_index_name(self):
        tz = dateutil.parser.parse(self.date_valid)
        vars = {'year':tz.year, 'month': tz.month, 'day': tz.day}
        return self.index_name.format(**vars)

