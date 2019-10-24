# -*- coding: utf-8 -*-
"""BigQuery Load Task."""
from pathlib import Path
from typing import Dict, List

from google.cloud import bigquery

from luft.common.column import Column
from luft.common.config import (
    BQ_DATA_TYPES, BQ_HIST_DEFAULT_TEMPLATE, BQ_STAGE_DEFAULT_TEMPLATE,
    BQ_STAGE_SCHEMA_FORM, GCS_BUCKET, PATH_PREFIX)
from luft.common.logger import setup_logger
from luft.common.utils import NoneStr, get_path_prefix
from luft.tasks.bq_exec_task import BQExecTask

import pkg_resources

# Setup logger
logger = setup_logger('common', 'INFO')


class BQLoadTask(BQExecTask):
    """BQ Load Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 columns: List[Column], project_id: NoneStr = None, location: NoneStr = None,
                 dataset_id: NoneStr = None, skip_leading_rows: bool = True,
                 allow_quoted_newlines: bool = True, disable_check: bool = False,
                 field_delimiter: str = '\t', path_prefix: NoneStr = None,
                 yaml_file: NoneStr = None, env: NoneStr = None,
                 thread_name: NoneStr = None, color: NoneStr = None):
        """Initialize BigQuery Load Task.

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
        self.path_prefix = path_prefix or PATH_PREFIX
        self.skip_leading_rows = skip_leading_rows
        self.allow_quoted_newlines = allow_quoted_newlines
        self.field_delimiter = field_delimiter
        self.disable_check = disable_check
        self.dataset_id = dataset_id or source_system
        self.stage_dataset_id = BQ_STAGE_SCHEMA_FORM.format(
            env=self.get_env,
            source_system=self.get_source_system,
            source_subsystem=self.get_source_subsystem,
            dataset_id=self.dataset_id
        )
        super().__init__(name=name, task_type=task_type,
                         source_system=source_system,
                         source_subsystem=source_subsystem,
                         yaml_file=yaml_file,
                         project_id=project_id,
                         location=location,
                         env=env, thread_name=thread_name, color=color)

    def __call__(self, ts: str, env: NoneStr = None):
        """Make class callable.

        Attributes:
            ts (str): time of valid.

        """
        stage_template = Path(pkg_resources.resource_filename(
            'luft', BQ_STAGE_DEFAULT_TEMPLATE))
        hist_template = Path(pkg_resources.resource_filename(
            'luft', BQ_HIST_DEFAULT_TEMPLATE))
        env_vars = self.get_env_vars(ts, env)
        self._create_dataset(self.stage_dataset_id)
        self._run_bq_command(stage_template.parent, [stage_template.name],
                             env_vars)
        self.load_csv()
        self._create_dataset(self.dataset_id)
        self._run_bq_command(hist_template.parent, [hist_template.name],
                             env_vars)

    def get_env_vars(self, ts: str, env: NoneStr = None) -> Dict[str, str]:
        """Get Docker enviromental variables."""
        super_env_dict = super().get_env_vars(ts=ts, env=env)
        env_dict = {
            'TABLE_NAME': self.name,
            'STAGE_SCHEMA': f'{self.source_system.lower()}_{self.source_subsystem.lower()}_staging',
            'HISTORY_SCHEMA': f'{self.source_system.lower()}_{self.source_subsystem.lower()}_history',
            'PK': self._get_col_names('pk'),
            'PK_DEFINITION_LIST': self._get_col_defs('pk'),
            'COLUMNS': self._get_col_names('nonpk'),
            'COLUMN_DEFINITION_LIST': self._get_col_defs('nonpk'),
            'HASH_COLUMNS': self._get_hash_diff(),
            'PK_JOIN': self._get_pk_join()
        }
        clean_dict = self.clean_dictionary(env_dict)
        clean_dict.update(super_env_dict)
        return clean_dict

    def _get_col_names(self, col_type: str) -> str:
        """Get list of column names.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned.

        """
        cols = [col.get_name(col_type, include_tech=False) for col in self.columns
                if col.get_name(col_type, include_tech=False)]
        return ',\n'.join(cols)

    def _get_col_defs(self, col_type: str) -> str:
        """Get list of column definition.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned.

        """
        cols = [col.get_def(col_type, include_tech=False, supported_types=BQ_DATA_TYPES)
                for col in self.columns if col.get_def(col_type, include_tech=False,
                                                       supported_types=BQ_DATA_TYPES)]
        return ',\n    '.join(cols)

    def _get_pk_join(self) -> str:
        """Get PK join.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned.

        """
        pk_join = [col.get_join('pk', include_tech=False) for col in self.columns
                   if col.get_join('pk', include_tech=False)]
        return '\nAND '.join(pk_join)

    def _get_hash_diff(self) -> str:
        """Get hash diff columns.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned.

        """
        cols = [f'TO_JSON_STRING({col.get_name("all", include_tech=False)})' for col
                in self.columns if col.get_name('all', include_tech=False)]
        return ', '.join(cols)

    def load_csv(self):
        """Load CSV."""
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = int(self.skip_leading_rows)
        job_config.allow_quoted_newlines = self.allow_quoted_newlines
        job_config.field_delimiter = self.field_delimiter
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        table_ref = self.bq_client.dataset(
            self.stage_dataset_id).table(self.get_name())
        uri = f'gs://{GCS_BUCKET}/' + get_path_prefix(path_prefix=self.path_prefix,
                                                      env=self.get_env(),
                                                      source_system=self.get_source_system(),
                                                      source_subsystem=self.get_source_subsystem(),
                                                      name=self.get_name(),
                                                      date_valid=self.get_date_valid(),
                                                      time_valid=self.get_time_valid()
                                                      ) + '*'
        logger.info(f'Loading CSV data from `{uri}`.')
        load_job = self.bq_client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )
        try:
            load_job.result()
            stage_table = self.bq_client.get_table(table_ref)
            logger.info(
                f'Loaded {stage_table.num_rows} rows into {self.get_name()}.')
            if self.disable_check and stage_table.num_rows == 0:
                raise TypeError(
                    f'There is no data in {self.stage_dataset_id + "." + self.get_name()}.')
        except Exception as e:
            logger.error(e)
            logger.error(load_job.errors)
            raise
